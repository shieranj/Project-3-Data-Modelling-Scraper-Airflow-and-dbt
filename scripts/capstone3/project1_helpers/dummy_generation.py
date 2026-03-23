from faker import Faker
import random
from datetime import datetime
import pandas as pd
import uuid
import string
import os
# fsspec gcsfs

fake = Faker('id_ID')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# customer_id(PK), name, email, birthday, phone_number, address, join_date, created_at
def generate_new_customers(num_of_cust=100, execution_date=None):
    #return file path gcs

    customers = []

    if execution_date is None:
        execution_date = datetime.now()
    try:
        
        for i in range(num_of_cust):
            
            customers.append({
                "customer_id": f"CUST_{str(uuid.uuid4())}",
                "name": fake.name(),
                "email": fake.email(),
                "birthday": fake.date_of_birth(),
                "phone_number": fake.phone_number(),
                "address": fake.address(),
                "join_date": fake.date_between(start_date="-2y", end_date="-2d"),
                "created_at": execution_date,
                "updated_at": execution_date
            })
        
        customers_df = pd.DataFrame(customers)
        print(f"Customers generated! rows: {len(customers_df)}")

        file_name = f"customers_{execution_date.strftime('%Y%m%d%H%M%S')}.csv"
        gcs_path = f"gs://shieran-gcs-bucket/capstone3/project1/postgres_temp/customers/{file_name}"

        print(f"Saving to GCS {gcs_path} ....")
        customers_df.to_csv(gcs_path, index = False)
        print(f"Successfully saved customers data to {gcs_path}")

        return gcs_path

    except Exception as e:
        print(f"Error generating customer data! {e}")
        raise
# transaction_id(PK), cust_id(FK), product_id, qty, order_amount, created_at
def generate_new_transactions(customers_gcs_path, execution_date=None):

    transactions = []

    if customers_gcs_path is None:
        print(f"GCS path not obtained! Exiting function...")
        raise

    if execution_date is None:
        execution_date = datetime.now()
    
    customers_df = pd.read_csv(customers_gcs_path)
    try:

        customer_ids = customers_df["customer_id"].tolist()
        
        products_gcs_path = "gs://shieran-gcs-bucket/capstone3/project1/postgres_temp/products/retail_products.csv"
        products_df = pd.read_csv(products_gcs_path)
        product_ids = products_df["product_id"].tolist()

        for cust in customer_ids:
            
            product_id = random.choice(product_ids)
            unit_price = products_df.loc[products_df["product_id"] == product_id, "price"].iloc[0]
            qty = random.randint(1,5)
            total_amount = unit_price*qty

            transactions.append({
                "trx_id" :  f"TRX_{str(uuid.uuid4())}",
                "customer_id": cust,
                "product_id" : product_id,       
                "unit_price": unit_price,
                "quantity": qty,
                "total_amount": total_amount,
                "created_at": execution_date,
                "updated_at": execution_date
            }
            )

        transactions_df = pd.DataFrame(transactions)
        print(f"Transaction data generated! rows: {len(transactions_df)}")

        file_name = f"transactions_{execution_date.strftime('%Y%m%d%H%M%S')}.csv"
        gcs_path = f"gs://shieran-gcs-bucket/capstone3/project1/postgres_temp/transactions/{file_name}"

        print(f"Saving to GCS {gcs_path}....")
        transactions_df.to_csv(gcs_path, index=False)
        print(f"Successfully saved transaction data {gcs_path}")
        return gcs_path
    
    except Exception as e:
        print(f"Error creating transactions table! {e}")
        raise

# payment_id(PK), trx_id(FK), amount, payment_method
def generate_new_payments(transactions_gcs_path, execution_date = None):

    if transactions_gcs_path is None:
        print(f"GCS path not obtained! Exiting function...")
        raise

    if execution_date is None:
        execution_date = datetime.now()

    transactions_df = pd.read_csv(transactions_gcs_path)

    payments = []
    payment_counter = 1
    methods = ["E_WALLET", "BANK_TRANSFER", "CREDIT_CARD", "CASH_ON_DELIVERY"]

    try:

        for row in transactions_df.itertuples(index = False):
            remaining = row.total_amount

            #split payment
            while remaining > 0:
                if remaining <= 100_000:
                    pay_amount = remaining
                else:
                    pay_amount = random.randint(100_000, remaining)

                payments.append({
                    "payment_id": f"PAY_{str(uuid.uuid4())}",
                    "trx_id": row.trx_id,
                    "amount": pay_amount,
                    "payment_method": random.choice(methods),
                    "created_at": execution_date,
                    "updated_at": execution_date
                })
            
                remaining -= pay_amount
                payment_counter += 1

        payment_df = pd.DataFrame(payments)
        print(f"Payment data generated! rows: {len(payment_df)}")

        file_name = f"payments_{execution_date.strftime('%Y%m%d%H%M%S')}.csv"
        gcs_path = f"gs://shieran-gcs-bucket/capstone3/project1/postgres_temp/payments/{file_name}"
        print(f"Saving to GCS {gcs_path}...")
        payment_df.to_csv(gcs_path, index=False)

        print(f"Successfully saved payments data to {gcs_path}")
        return gcs_path
    
    except Exception as e:
        print(f"Error generating payment data! {e}")
        raise
    




