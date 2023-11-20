from ksuid import ksuid
from decimal import Decimal
import datetime
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

def safe_db(func):
    def wrapper():
        try:
            response = func()
        except ClientError as err:
            logger.error(
                "%s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        return response
    return wrapper

def make_id(override_id=None):
    if override_id:
        return override_id
    else:
        return str(ksuid())
    
class EntityAttribute():
    def __init__(self, label, attributes={}, created_at = None, updated_at = None):
        self.label = label
        if created_at:
            self.created_at = created_at
        else:
            self.created_at = str(datetime.datetime.now()) 
        
        if updated_at:
            self.updated_at = updated_at
        else:
            self.updated_at = str(datetime.datetime.now())
        
        self.attributes = attributes
        
    def attr(self):
        attr = {
            "label": self.label, 
            "created_at": self.created_at,
            "updated_at": str(datetime.datetime.now())
        }
        attr.update(self.attributes)
        
        return attr

class CompositeKey():
    def __init__(self, pk, sk, is_composite):
        self.PK = pk
        self.SK = sk
        self.is_composite = is_composite

    def item_keys(self, pk_id, sk_id):
        if self.is_composite:
            item = {
                    "PK": "%s#%s/%s#%s" % (self.PK, pk_id,self.SK,sk_id),
                    "SK": "%s#%s" % (self.SK, sk_id) 
                }
        else:
            item = {
                    "PK": "%s#%s" % (self.PK, pk_id),
                    "SK": "%s#%s" % (self.SK, sk_id) 
                }            
        return item
    
    def parent_key(self, pk_id):
        item = {
                "PK": "%s#%s" % (self.PK, pk_id),
                "SK": "%s#%s" % (self.PK, pk_id) 
            }
        return item        

    def unique_label_keys(self, pk_id, sk_id):
        if self.is_composite:
            item = {
                "PK": "@ul\%s#%s/%s" % (self.PK, pk_id, self.SK),
                "SK": "%s" % (sk_id) 
            }
        else:
            item = {
                "PK": "@ul\%s" % (self.PK),
                "SK": "%s" % (sk_id) 
            }            

        return item  
    
class EntityItem():
    def __init__(self, pk, pk_id, sk=None, sk_id=None, attr=EntityAttribute(label=None)):

        self.PK = pk
        if sk:
            self.SK = sk
            self.is_composite = True
        else:
            self.SK = self.PK
            self.is_composite = False

        self.pk_id = pk_id

        if sk_id:
            self.sk_id = sk_id
        else:
            self.sk_id = pk_id 

        self.attr = attr.attr()

    def __dict__(self):
        return {"PK": self.PK, "SK": self.SK, "pk_id": self.pk_id, "sk_id": self.sk_id, "attr": self.attr}
    
    def __str__(self):
        return str(self.__dict__())

class Entity():
    """
    Ok fine
    """
    def __init__(self, client, pk=None, sk=None, parent=None):
        if pk:
            self.PK = pk
        else:
            self.PK = parent.PK
        if sk:
            self.SK = sk
            self.is_composite = True # project has workspaces
        else:
            self.SK = pk
            self.is_composite = False
        
        self.composite_key = CompositeKey(pk=self.PK, sk=self.SK, is_composite=self.is_composite)

        self.client = client
        self.parent = parent
        self.table = client.boto3_client.Table(client.table_name)
        self.table.load()  
    
    def get(self, entity_item):
        item = self.composite_key.item_keys(entity_item.pk_id, entity_item.sk_id)

        @safe_db
        def get_item(): 
            return self.table.get_item(Key=item,ConsistentRead=True)
        
        return get_item()

    def rename(self, entity_item):
        #change the name and deal with exclusitivity with new one and delete operation all in transaction
        pass

    def update(self, entity_item, update_expression, expression_attribut_values):

        key = self.item_keys(entity_item.pk_id, entity_item.sk_id)

        @safe_db
        def update_item():
            return self.table.update_item(
                Key=key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribut_values,
                ReturnValues="UPDATED_NEW"
            )
        
        return update_item()
    
    def inc(self, entity_item, attribute):
        @safe_db
        def inc_item():
            self.client.boto3_client.meta.client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": self.client.table_name,
                            "Key": self.composite_key.item_keys(entity_item.pk_id, entity_item.sk_id),
                            "UpdateExpression": "ADD #val :inc",
                            "ExpressionAttributeNames": {
                                "#val": attribute
                            },
                            "ExpressionAttributeValues": {
                                ":inc": 1
                            }
                        }
                    }    
                ]
            ) 

        return inc_item()                  

    def decr(self, entity_item, attribute):
        @safe_db
        def dec_item():
            return self.client.boto3_client.meta.client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": self.client.table_name,
                            "Key": self.composite_key.item_keys(entity_item.pk_id, entity_item.sk_id),
                            "UpdateExpression": "ADD #val :inc ",
                            "ExpressionAttributeNames": {
                                "#val": attribute
                            },
                            "ExpressionAttributeValues": {
                                ":inc": -1
                            }
                        }
                    }    
                ]
            )   
                 
        return dec_item()
    
    def create(self, attributes, override_id=None, parent_counter=None):
        if self.is_composite:
            pk_id = self.parent.pk_id
            sk_id = make_id(override_id)
        else:
            pk_id = make_id(override_id)
            sk_id = pk_id
        
        
        put_key = self.composite_key.item_keys(pk_id, sk_id)
        put_key.update(attributes.attr())
        put_key.update({"id": sk_id})

        unique_id = sk_id
        unique_name = self.composite_key.unique_label_keys(pk_id, attributes.label)
        unique_name.update({"id":unique_id}) # so we have the ID to look up the meta data

        items_to_create = [{
                                "Put": {
                                    "TableName": self.client.table_name,
                                    "Item": put_key,    
                                    "ConditionExpression": "attribute_not_exists(PK) and attribute_not_exists(SK)"                        
                                }
                            },
                            {
                                "Put": {
                                    "TableName": self.client.table_name,
                                    "Item": unique_name,      
                                    "ConditionExpression": "attribute_not_exists(PK) and attribute_not_exists(SK)"
                                } 
                            }]
        
        if self.is_composite and parent_counter:
            parent_key = self.composite_key.parent_key(self.parent.pk_id)
            kv = {
                    "Update": {
                        "TableName": self.client.table_name,
                        "Key": parent_key,
                        "UpdateExpression": "ADD #val :inc ",
                        "ExpressionAttributeNames": {
                            "#val": parent_counter
                        },
                        "ExpressionAttributeValues": {
                            ":inc": 1
                        }
                    }
                }   
            items_to_create.append(kv)

        @safe_db
        def write_item():
            self.client.boto3_client.meta.client.transact_write_items(
                TransactItems=items_to_create
            )    

        write_item()

        return EntityItem(pk=self.PK, pk_id=pk_id, sk=self.SK, sk_id=sk_id, attr=attributes)                                      
   
    def all(self, entity_item=None, return_cols=[]):
        scan_kwargs = {
            "FilterExpression": "#pk between :start and :end",
            "ExpressionAttributeNames": {"#pk": "PK"},
        }

        if self.is_composite:
            expression_attribute_values = {"ExpressionAttributeValues": {
                ":start": "%s#%s/%s" % (self.PK, entity_item.pk_id, self.SK),
                ":end": "%s#%s/%s$" % (self.PK, entity_item.pk_id, self.SK)
            }}
        else:
            expression_attribute_values = {"ExpressionAttributeValues": {
                ":start": "%s" % (self.PK),
                ":end": "%s$" % (self.PK)
            }}
        scan_kwargs.update(expression_attribute_values)            

        if return_cols:
            scan_kwargs.update({"ProjectionExpression": ", ".join(return_cols)})
        #else return all columns which may not be ideal

        return self.scan_table(scan_kwargs=scan_kwargs)

    def some(self, entity_item, start, end, return_cols=[]):
        scan_kwargs = {
            "FilterExpression": "#pk = :pk and (#sk between :start and :end)",
            "ExpressionAttributeNames": {"#pk": "PK", "#sk": "SK"},
        }

        if self.is_composite:
            expression_attribute_values = {"ExpressionAttributeValues": {
                ":pk": "%s#%s/%s" % (self.PK, entity_item.pk_id, self.SK),
                ":start": "%s#%s" % (self.PK, start),
                ":end": "%s#%s" % (self.PK, end)
            }}
        else:
            expression_attribute_values = {"ExpressionAttributeValues": {
                ":pk": "%s#%s" % (self.PK, entity_item.pk_id),
                ":start": "%s#%s" % (self.PK, start),
                ":end": "%s#%s" % (self.PK, end)
            }}
        scan_kwargs.update(expression_attribute_values)            

        if return_cols:
            scan_kwargs.update({"ProjectionExpression": ", ".join(return_cols)})
        #else return all columns which may not be ideal

        return self.scan_table(scan_kwargs=scan_kwargs)
    
    def scan_table(self, scan_kwargs):
        results = []
        done = False
        start_key = None
        
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            
            @safe_db
            def do_scan():
                return self.table.scan(**scan_kwargs)
            
            response = do_scan()
            results.extend(response.get("Items", []))
            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None
        
        return results
