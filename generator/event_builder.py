import uuid
from datetime import datetime, timezone

class EventBuilder:
    def __init__(self, source="web", event_version="1.0"):
        self.source = source
        self.event_version = event_version
        
    def _base_header(self, event_type):
        return {
            "event_id": str(uuid.uuid4()),
            "event_version": self.event_version,
            "event_type": event_type,
            "event_timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "producer": "event-generator",
            "source": self.source
        }
        
    def build_search_event(self, customer_id, session_id, search_query, country="Brazil"):
        payload = self._base_header("search")
        payload.update({
            "customer_id": str(customer_id),
            "session_id": str(session_id),
            "search_query": search_query,
            "country": country
        })
        return "search-events", payload

    def build_product_view_event(self, customer_id, session_id, product_id, category, device="mobile", country="Brazil"):
        payload = self._base_header("product_view")
        payload.update({
            "customer_id": str(customer_id),
            "session_id": str(session_id),
            "product_id": str(product_id),
            "category": category,
            "device": device,
            "country": country
        })
        return "product-view-events", payload

    def build_cart_event(self, event_type, customer_id, session_id, product_id, quantity, unit_price):
        # event_type can be add_to_cart or remove_from_cart
        payload = self._base_header(event_type)
        payload.update({
            "customer_id": str(customer_id),
            "session_id": str(session_id),
            "product_id": str(product_id),
            "quantity": int(quantity),
            "unit_price": float(unit_price),
            "cart_value": round(float(unit_price) * int(quantity), 2)
        })
        return "cart-events", payload

    def build_checkout_event(self, customer_id, session_id, order_id, total_amount, item_count):
        payload = self._base_header("checkout_started")
        payload.update({
            "customer_id": str(customer_id),
            "session_id": str(session_id),
            "order_id": str(order_id),
            "total_amount": round(float(total_amount), 2),
            "item_count": int(item_count),
            "currency": "BRL"
        })
        return "checkout-events", payload

    def build_payment_event(self, customer_id, order_id, amount, status="SUCCESS", payment_method="Credit Card"):
        # status: SUCCESS or FAILED
        evt_type = "payment_completed" if status == "SUCCESS" else "payment_failed"
        payload = self._base_header(evt_type)
        payload.update({
            "customer_id": str(customer_id),
            "order_id": str(order_id),
            "payment_method": payment_method,
            "payment_status": status,
            "amount": round(float(amount), 2),
            "currency": "BRL"
        })
        return "payment-events", payload

    def build_order_completed_event(self, customer_id, order_id, total_amount, items):
        payload = self._base_header("order_completed")
        payload.update({
            "customer_id": str(customer_id),
            "order_id": str(order_id),
            "total_amount": round(float(total_amount), 2),
            "items": items,
            "status": "DELIVERY_PENDING"
        })
        return "order-events", payload

    def build_review_event(self, customer_id, order_id, product_id, rating, review_text=""):
        payload = self._base_header("review_submitted")
        payload.update({
            "customer_id": str(customer_id),
            "order_id": str(order_id),
            "product_id": str(product_id),
            "rating": int(rating),
            "review_text": review_text
        })
        return "review-events", payload

    def build_inventory_event(self, product_id, warehouse_id, old_qty, new_qty):
        payload = self._base_header("inventory_updated")
        payload.update({
            "product_id": str(product_id),
            "warehouse_id": int(warehouse_id),
            "old_quantity": int(old_qty),
            "new_quantity": int(new_qty)
        })
        return "inventory-events", payload
