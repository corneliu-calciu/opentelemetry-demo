from locust import HttpUser, task, constant_pacing
import random

class RecommendationUser(HttpUser):
    # 10 requests per second = 0.01 seconds between requests
    wait_time = constant_pacing(0.2)
    
    # @task
    # def get_recommendations(self):
    #     products = [
    #         "0PUK6V6EV0",
    #         "1YMWWN1N4O",
    #         "2ZYFJ3GM2N",
    #         "66VCHSJNUP",
    #         "6E92ZMYYFZ",
    #         "9SIQT8TOJO",
    #         "L9ECAV7KIM",
    #         "LS4PSXUNUM",
    #         "OLJCESPC7Z",
    #         "HQTGWGPNH4",
    #     ]

    #     # Randomly choose between the two product IDs
    #     product_id = random.choice(products)
        
    #     # The API endpoint with single product ID
    #     url = f"/api/recommendations?productIds={product_id}"        
        
    #     # Perform GET request
    #     with self.client.get(url, catch_response=True, name="get_recommendations-" + product_id) as response:
    #         if response.status_code == 200:
    #             response.success()
    #         else:
    #             response.failure(f"Status code: {response.status_code}")
    
    @task
    def get_product(self):
        products = [
            "OLJCESPC7Z",
        ]
        # Randomly choose between the two product IDs
        product_id = random.choice(products)
        
        # The API endpoint with single product ID
        #url = f"/api/recommendations?productIds={product_id}"
        #
        url = f"http://devenv:8080/product/{product_id}"
        
        # Perform GET request
        with self.client.get(url, catch_response=True, name="get_product-" + product_id) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")

    # Optional: Add a task to test with both products simultaneously
    # @task(1)  # This will run less frequently (weight=1 vs default weight=1)
    # def get_recommendations_both(self):
    #     # API endpoint with both product IDs
    #     url = "/api/recommendations?productIds=OLJCESPC7Z,66VCHSJNUP"
        
    #     # Perform GET request
    #     with self.client.get(url, catch_response=True, name="get_recommendations_both") as response:
    #         if response.status_code == 200:
    #             response.success()
    #         else:
    #             response.failure(f"Status code: {response.status_code}")
    
    # Optional: Add host configuration
    host = "http://devenv:8080"