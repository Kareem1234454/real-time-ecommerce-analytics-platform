import os
import random
import uuid

class SessionController:
    def __init__(self, customers_df, products_df, event_builder, probabilities):
        self.customers_df = customers_df
        self.products_df = products_df
        self.eb = event_builder
        self.probs = probabilities
        self.active_sessions = {}
        
        # Cache lists for rapid O(1) random sampling during simulation
        self.customer_records = self.customers_df.to_dict("records")
        self.product_records = self.products_df.to_dict("records")
        self.devices = ["mobile", "desktop", "tablet"]
        self.search_keywords = [
            "smart tv 4k", "wireless bluetooth earbuds", "gaming laptop", 
            "espresso machine", "running shoes", "smartphone cases",
            "leather wallet", "smart watch", "ergonomic office chair"
        ]
        self.payment_methods = ["Credit Card", "Boleto", "Pix", "Debit Card", "Voucher"]
        
    def _start_new_session(self):
        cust = random.choice(self.customer_records)
        session_id = f"S-{uuid.uuid4().hex[:8].upper()}"
        session_state = {
            "session_id": session_id,
            "customer_id": cust["customer_id"],
            "city": cust.get("city", "Sao Paulo"),
            "device": random.choice(self.devices),
            "stage": "SEARCH", # SEARCH -> VIEW -> CART -> CHECKOUT -> PAYMENT -> ORDER -> REVIEW
            "cart_items": [],
            "cart_total": 0.0,
            "current_product": random.choice(self.product_records),
            "order_id": None
        }
        self.active_sessions[session_id] = session_state
        return session_state

    def advance_session(self, inject_fraud=False):
        if not self.active_sessions or len(self.active_sessions) < 20 or random.random() < 0.3:
            sess = self._start_new_session()
        else:
            session_id = random.choice(list(self.active_sessions.keys()))
            sess = self.active_sessions[session_id]
            
        stage = sess["stage"]
        cid = sess["customer_id"]
        sid = sess["session_id"]
        prod = sess["current_product"]
        
        # 1. Search Stage
        if stage == "SEARCH":
            topic, payload = self.eb.build_search_event(cid, sid, random.choice(self.search_keywords))
            sess["stage"] = "VIEW"
            return topic, payload

        # 2. View Product Stage
        elif stage == "VIEW":
            topic, payload = self.eb.build_product_view_event(
                cid, sid, prod["product_id"], prod.get("category", "General"), device=sess["device"]
            )
            if random.random() < self.probs.get("cart_addition", 0.65):
                sess["stage"] = "CART"
            else:
                # Browse another item or exit
                if random.random() < 0.5:
                    sess["current_product"] = random.choice(self.product_records)
                else:
                    del self.active_sessions[sid]
            return topic, payload

        # 3. Add to Cart Stage
        elif stage == "CART":
            qty = random.randint(1, 3)
            unit_p = prod.get("unit_price", 79.99)
            topic, payload = self.eb.build_cart_event("add_to_cart", cid, sid, prod["product_id"], qty, unit_p)
            sess["cart_items"].append({"product_id": prod["product_id"], "quantity": qty, "price": unit_p})
            sess["cart_total"] += round(qty * unit_p, 2)
            
            # Check for Cart Abandonment
            if random.random() < self.probs.get("cart_abandonment", 0.25):
                del self.active_sessions[sid]
            else:
                sess["stage"] = "CHECKOUT"
            return topic, payload

        # 4. Checkout Stage
        elif stage == "CHECKOUT":
            order_id = f"ORD-{uuid.uuid4().hex[:10].upper()}"
            sess["order_id"] = order_id
            topic, payload = self.eb.build_checkout_event(
                cid, sid, order_id, sess["cart_total"], len(sess["cart_items"])
            )
            sess["stage"] = "PAYMENT"
            return topic, payload

        # 5. Payment Stage (With Fraud Injection Support)
        elif stage == "PAYMENT":
            oid = sess.get("order_id") or f"ORD-{uuid.uuid4().hex[:10].upper()}"
            method = random.choice(self.payment_methods)
            
            if inject_fraud or random.random() > self.probs.get("payment_success", 0.94):
                # Payment failure / Fraud suspicious card test
                topic, payload = self.eb.build_payment_event(cid, oid, sess["cart_total"], status="FAILED", payment_method="Credit Card")
                if not inject_fraud:
                    del self.active_sessions[sid]
                # If fraud injected, we keep stage at PAYMENT to simulate rapid repeated attempts!
            else:
                topic, payload = self.eb.build_payment_event(cid, oid, sess["cart_total"], status="SUCCESS", payment_method=method)
                sess["stage"] = "ORDER_COMPLETE"
            return topic, payload

        # 6. Order Complete Stage
        elif stage == "ORDER_COMPLETE":
            oid = sess["order_id"]
            topic, payload = self.eb.build_order_completed_event(cid, oid, sess["cart_total"], sess["cart_items"])
            sess["stage"] = "REVIEW"
            return topic, payload

        # 7. Review Stage
        elif stage == "REVIEW":
            oid = sess["order_id"]
            rating = random.choices([5, 4, 3, 2, 1], weights=[55, 25, 10, 5, 5])[0]
            reviews_map = {
                5: "Excellent delivery and amazing quality! Totally recommended.",
                4: "Very good product, arrived on time.",
                3: "Average quality, matches description.",
                2: "Delivery took longer than expected.",
                1: "Defective item, requesting return."
            }
            topic, payload = self.eb.build_review_event(
                cid, oid, prod["product_id"], rating, review_text=reviews_map[rating]
            )
            del self.active_sessions[sid]
            return topic, payload

        # Default fallback
        sess["stage"] = "SEARCH"
        return self.eb.build_search_event(cid, sid, "general shopping")
