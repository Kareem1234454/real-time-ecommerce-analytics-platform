import os
import sys
import io
import json
import re
import time
import requests
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    from hdfs import InsecureClient
    from hdfs.util import HdfsError
except ImportError:
    InsecureClient = None
    HdfsError = None

class WebHDFSLocalhostSession(requests.Session):
    """
    HTTP Session interceptor for WebHDFS on Windows.
    Automatically catches Hadoop 307 Temporary Redirects pointing to unresolvable Docker internal hostnames
    or container IP addresses (e.g., hadoop-datanode:9864 or 172.x.x.x:9864) and rewrites them directly to localhost:9864.
    """
    def __init__(self, target_host="localhost"):
        super().__init__()
        self.target_host = target_host

    def send(self, request, **kwargs):
        if any(port in request.url for port in [":9864", ":9866", ":9867", ":50075", ":9870"]):
            request.url = re.sub(r"http://[^:]+:(9864|9866|9867|50075|9870)", rf"http://{self.target_host}:\1", request.url)
        return super().send(request, **kwargs)

class HDFSClient:
    """
    Enterprise Unified HDFS Client Wrapper for Medallion Distributed Data Lake Architecture.
    Communicates with containerized Apache Hadoop NameNode via high-performance WebHDFS REST endpoints (Port 9870),
    eliminating the requirement for Windows Java installations or WinUtils binaries.
    """
    def __init__(self, namenode_url=None, user="root", root_prefix="/data_lake"):
        if namenode_url is None:
            namenode_url = os.getenv("HDFS_NAMENODE_URL", "http://localhost:9870")
        self.namenode_url = namenode_url
        self.user = user
        self.root_prefix = root_prefix
        self.client = None
        self._init_client()

    def _init_client(self):
        if InsecureClient is not None:
            try:
                session = WebHDFSLocalhostSession("localhost")
                self.client = InsecureClient(self.namenode_url, user=self.user, session=session)
            except Exception as e:
                self.client = None

    def is_available(self):
        """Verify HDFS NameNode connectivity and operational readiness."""
        if not self.client:
            self._init_client()
        if self.client:
            try:
                self.client.status("/", strict=False)
                return True
            except Exception:
                pass
        return False

    def makedirs(self, hdfs_path):
        """Recursively create partitioned directory structure inside HDFS storage blocks."""
        if not self.client:
            return False
        try:
            full_path = self._norm_path(hdfs_path)
            self.client.makedirs(full_path)
            return True
        except Exception:
            return False

    def _norm_path(self, path):
        path_str = str(path).replace("\\", "/")
        if not path_str.startswith("/"):
            path_str = f"/{path_str}"
        return path_str

    def append_jsonl(self, hdfs_dir, filename, payload):
        """Append streaming JSON event payload directly into an HDFS Bronze log file."""
        if not self.client:
            return False
        try:
            dir_path = self._norm_path(hdfs_dir)
            file_path = f"{dir_path}/{filename}".replace("//", "/")
            self.client.makedirs(dir_path)
            
            line = json.dumps(payload, ensure_ascii=False) + "\n"
            exists = self.client.status(file_path, strict=False)
            if exists:
                self.client.write(file_path, data=line.encode("utf-8"), append=True)
            else:
                self.client.write(file_path, data=line.encode("utf-8"), overwrite=True)
            return True
        except Exception as e:
            # Fallback to overwrite if append unsupported during NameNode transition
            try:
                self.client.write(file_path, data=line.encode("utf-8"), overwrite=True)
                return True
            except Exception:
                return False

    def write_parquet(self, hdfs_path, df):
        """Serialize a Pandas DataFrame directly to HDFS storage block via memory buffer."""
        if not self.client or df is None or df.empty:
            return False
        try:
            full_path = self._norm_path(hdfs_path)
            parent_dir = "/".join(full_path.split("/")[:-1])
            if parent_dir:
                self.client.makedirs(parent_dir)
                
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            self.client.write(full_path, data=buf, overwrite=True)
            return True
        except Exception as e:
            return False

    def list_files_recursive(self, hdfs_dir, extension=None):
        """Recursively traverse HDFS directory structure returning all matching file paths."""
        if not self.client:
            return []
        matched_files = []
        try:
            dir_path = self._norm_path(hdfs_dir)
            if not self.client.status(dir_path, strict=False):
                return []
                
            def _walk(path):
                for fname, meta in self.client.list(path, status=True):
                    subpath = f"{path}/{fname}".replace("//", "/")
                    if meta["type"] == "DIRECTORY":
                        _walk(subpath)
                    else:
                        if extension is None or fname.endswith(extension):
                            matched_files.append((subpath, meta["modificationTime"]))
            _walk(dir_path)
            # Sort by modification time descending (newest first)
            matched_files.sort(key=lambda x: x[1], reverse=True)
            return [f[0] for f in matched_files]
        except Exception:
            return []

    def read_parquet_files(self, hdfs_dir, latest_only=False):
        """Read Parquet datasets from HDFS blocks directly into memory DataFrame."""
        if not self.client or pd is None:
            return pd.DataFrame() if pd else None
        try:
            files = self.list_files_recursive(hdfs_dir, extension=".parquet")
            if not files:
                return pd.DataFrame()
                
            if latest_only:
                files = [files[0]]
                
            df_list = []
            for fpath in files:
                try:
                    with self.client.read(fpath) as reader:
                        df_part = pd.read_parquet(io.BytesIO(reader.read()))
                        if not df_part.empty:
                            df_list.append(df_part)
                except Exception:
                    pass
                    
            if not df_list:
                return pd.DataFrame()
            return pd.concat(df_list, ignore_index=True)
        except Exception:
            return pd.DataFrame() if pd else None

    def read_jsonl_events(self, hdfs_dir, max_files=5, max_lines_per_file=50):
        """Fetch streaming JSON event lines from Bronze HDFS blocks directly into memory list."""
        if not self.client:
            return []
        events = []
        try:
            files = self.list_files_recursive(hdfs_dir, extension=".jsonl")
            for fpath in files[:max_files]:
                try:
                    with self.client.read(fpath, encoding="utf-8") as reader:
                        lines = reader.read().splitlines()
                        for line in lines[-max_lines_per_file:]:
                            if line.strip():
                                events.append(json.loads(line.strip()))
                except Exception:
                    pass
            return events
        except Exception:
            return []

# Singleton export for easy import across platform engines
hdfs = HDFSClient()
