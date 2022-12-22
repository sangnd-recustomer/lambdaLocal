import json

from order_webhook import (
    webhook_try_order_group,
)


def lambda_handler(event, context):
    print('================================PAYLOAD================================')

    data_payload: object = event['detail']['payload']

    type_webhook = ''
    metadata: object = event['detail']['metadata']
    if 'detail' in event and metadata['X-Shopify-Topic']:
        type_webhook: str = metadata['X-Shopify-Topic']

    if type_webhook == 'orders/create':
        print('================================Create orders================================')
        payment_terms: object = data_payload.get('payment_terms', None)
        payment_method: str = data_payload.get('gateway', None)
        financial_status: str = data_payload.get('financial_status')
        if (
                payment_terms and
                payment_method == 'shopify_payments' and
                hasattr(data_payload, "payment_details") and
                financial_status == 'pending'
        ):
            webhook_try_order_group(data_payload)

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Listen event webhook Shopify!')
    }
