# credential to access s3 bucket for uploads

from _general_funcs.utils import get_dbutils

AWS_CREDS = {
        'aws_access_key_id': get_dbutils().secrets.get(scope = 'ds_credentials', key = 'aws_oa_prov_access_key_id'),
        'aws_secret_access_key': get_dbutils().secrets.get(scope = 'ds_credentials', key = 'aws_oa_prov_secret_access_key')
    }