import os
import pymysql
from datetime import timezone, datetime, timedelta
import uuid

DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')


def get_rds_connection():
    """
    Get connection to database
    """
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, passwd=DB_PASSWORD, db=DB_NAME,
        connect_timeout=5, charset='utf8', port=19088
    )


def webhook_try_order_group(data_payload: object):
    """
    Create try_order_group, try_order
    """

    try:
        print('=======START webhook_create_order=======')

        # get connect data
        conn = get_rds_connection()  # type: ignore

        order_status_url: str = data_payload['order_status_url']  # type: ignore
        store_url: str = order_status_url.split('/')[2]
        store = get_store(store_url, conn)
        shipping_address: object = data_payload.get('shipping_address', None)

        if len(store) == 1:  # type: ignore
            print('=======Starting get try_order_setting=======')
            try_order_setting = get_try_order_setting_by_store(store[0], conn)

            print('=======Starting save save_try_order_group=======')
            order_group_id: int = save_try_order_group(
                data_payload,
                int(store[0]),  # type: ignore
                shipping_address,
                conn,
                try_order_setting
            )

            print('order_group_id', order_group_id)

            print('=======Starting create try order=======')
            save_try_order(int(store[0]), data_payload, order_group_id, conn)  # type: ignore

        print('=======END webhook_create_order=======')
    except Exception as e:
        print('ERROR webhook_create_order', e)
        raise e
    finally:
        conn.close()  # type: ignore


def get_store(shopify_url: str, conn):  # type: ignore
    """
    Get store information
    """
    try:
        print('=======START get_store=======')
        query = """
                    SELECT id
                    FROM stores
                    WHERE JSON_EXTRACT(api_auth_details,'$.store_url') like '%{shopify_url}%';
                """.format(shopify_url=shopify_url)
        with conn.cursor() as cursor:  # type: ignore
            cursor.execute(query)
            print('=======END get_store=======')
            return cursor.fetchone()  # type: ignore
    except Exception as e:
        print('ERROR get_store', e)
        raise e


def get_try_order_setting_by_store(store_id: int, conn):  # type: ignore
    """
    Get try_order_settings information
    """
    try:
        print('=======START get_try_order_setting_by_store=======')
        store_id: int = store_id
        query = """
                    SELECT id, try_deadline, return_deadline, payment_deadline
                    FROM try_order_settings
                    WHERE store_id = {store_id};
                """.format(store_id=store_id)
        with conn.cursor() as cursor:  # type: ignore
            cursor.execute(query)
            print('=======END get_try_order_setting_by_store=======')
            return cursor.fetchone()  # type: ignore
    except Exception as e:
        print('ERROR get_try_order_setting_by_store', e)
        raise e


def save_try_order_group(  # type: ignore
        data_payload: object,
        store_id: int,  # type: ignore
        shipping_address: object,
        conn,  # type: ignore
        try_order_setting  # type: ignore
):
    """
    Get save try order group
    """

    try:
        print('=======START save_try_order_group=======')
        store_id: int = store_id
        cart_order_id: int = int(data_payload.get('id'))  # type: ignore
        cart_order_number: str = str(data_payload.get('name'))  # type: ignore
        request_id: str = str(data_payload.get('number'))  # type: ignore
        current_total_price: int = data_payload.get('current_total_price')
        payment_method: str = data_payload.get('gateway')
        order_date = datetime.fromisoformat(
            data_payload.get('created_at')  # type: ignore
        ).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        customer: object = data_payload.get('customer', None)
        email_address: str = ''
        customer_name: str = ''
        if customer:
            email_address: str = customer.get('email')
            first_name: str = customer.get('first_name', '')
            last_name: str = customer.get('last_name', '')
            customer_name = last_name + first_name

        postal_code = 0
        if shipping_address:
            postal_code: int = shipping_address.get('zip')

        DEFAULT_TRY_DEADLINE = 7
        DEFAULT_RETURN_DEADLINE = 10
        DEFAULT_PAYMENT_DEADLINE = 14

        if try_order_setting:
            DEFAULT_TRY_DEADLINE = try_order_setting[1]
            DEFAULT_RETURN_DEADLINE = try_order_setting[2]
            DEFAULT_PAYMENT_DEADLINE = try_order_setting[3]

        order_created_at = datetime.fromisoformat(data_payload.get('created_at')).astimezone(timezone.utc)
        try_deadline = (order_created_at + timedelta(days=DEFAULT_TRY_DEADLINE)).strftime("%Y-%m-%d %H:%M:%S")
        return_deadline = (order_created_at + timedelta(days=DEFAULT_RETURN_DEADLINE)).strftime("%Y-%m-%d %H:%M:%S")
        payment_deadline = (order_created_at + timedelta(days=DEFAULT_PAYMENT_DEADLINE)).strftime("%Y-%m-%d %H:%M:%S")

        idempotency_key = str(uuid.uuid4()).replace('-', '')

        query = """
                    INSERT INTO try_order_groups
                    (
                        store_id, cart_order_id, cart_order_number, request_id, current_total_price, customer_name,
                        payment_method, order_date, email_address, postal_code, created_at, updated_at,
                        is_archive, is_fixed, try_deadline, return_deadline, payment_deadline, idempotency_key,
                        return_status, purchase_status
                    )
                    VALUES(
                        {store_id}, {cart_order_id}, '{cart_order_number}', '{request_id}', {current_total_price},
                        '{customer_name}', '{payment_method}', '{order_date}', '{email_address}', '{postal_code}',
                        '{created_at}', '{updated_at}', {is_archive}, {is_fixed}, '{try_deadline}',
                        '{return_deadline}', '{payment_deadline}', '{idempotency_key}', '{return_status}',
                        '{purchase_status}'
                    );
                """.format(
            store_id=store_id, cart_order_id=cart_order_id, cart_order_number=cart_order_number, request_id=request_id,
            current_total_price=current_total_price, customer_name=customer_name, payment_method=payment_method,
            order_date=order_date, email_address=email_address, postal_code=postal_code, created_at=date_now,
            updated_at=date_now, is_archive=0, is_fixed=0, try_deadline=try_deadline, return_deadline=return_deadline,
            payment_deadline=payment_deadline, idempotency_key=idempotency_key, return_status='not_return',
            purchase_status='not_payment'
        )

        with conn.cursor() as cursor:  # type: ignore
            print('--------------query', query)
            cursor.execute(query)
            print("=======END save_try_order_group=======")
            return cursor.lastrowid  # type: ignore

    except Exception as e:
        print('ERROR save_try_order_group', e)
        raise e


def save_try_order(store_id: int, data_payload: object, order_group_id: int, conn):  # type: ignore
    """
    Save try order
    """
    try:
        print('=======START save_try_order=======')

        values = ''
        order_group_id: int = order_group_id
        store_id: int = store_id
        line_items = data_payload.get("line_items", None)  # type: ignore
        return_date = datetime.fromisoformat(
            data_payload.get('created_at')  # type: ignore
        ).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for line_item in line_items:  # type: ignore
            item_id: int = line_item.get('id')
            product_id: int = line_item.get('product_id')
            variant_id: int = line_item.get('variant_id')
            title: str = line_item.get('name')
            sku: str = line_item.get('sku')
            price: int = line_item.get('price')

            loop_quantity: int = line_item.get('quantity')

            for i in range(loop_quantity):
                values += f"""(
                    {store_id}, {order_group_id}, {item_id}, '{product_id}', {variant_id},
                    '{title}', '{sku}', {price}, '{return_date}', '{date_now}','{date_now}', 0
                ), """

        values = values.rstrip(", ")
        query = """
                    INSERT INTO try_orders
                    (
                        store_id, order_group_id, item_id, product_id, variant_id, title, sku, price, return_date,
                        created_at, updated_at, is_returned
                    )
                    VALUES{values};
                """.format(values=values)

        with conn.cursor() as cursor:  # type: ignore
            print('save_try_ordersave_try_ordersave_try_order1', query)
            cursor.execute(query)  # type: ignore
            conn.commit()
            print('=======END save_try_order=======')

    except Exception as e:
        print('ERROR save_try_order', e)
        raise e
