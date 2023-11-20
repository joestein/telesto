#!/usr/bin/env python3
import sys
import logging
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
from telesto.client import Client
from telesto.table import Entity, EntityAttribute
from ksuid import ksuid

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

class WorkspaceCategory(Entity):
    def __init__(self, client):
        pk="wc"
        super().__init__(client=client, pk=pk)

class Workspace(Entity):
    def __init__(self, client, parent):
        sk = "w"
        super().__init__(client=client, sk=sk, parent=parent)

boto3_client = boto3.resource("dynamodb", region_name="us-east-2")
client = Client(boto3_client, "pogo-main")

wc = WorkspaceCategory(client=client)
wc_ea = EntityAttribute(label=f"{str(ksuid())}", attributes={"test_attribute": {"color":"blue"}})

workspace_category_one = wc.create(wc_ea)
logger.info(workspace_category_one)
wc.inc(workspace_category_one, "testing_counter_example")
wc.inc(workspace_category_one, "testing_counter_example")
wc.inc(workspace_category_one, "testing_counter_example")

response = wc.get(workspace_category_one)
logger.info(response["Item"])

w = Workspace(client=client, parent=workspace_category_one)
w_ea = EntityAttribute(label=f"{str(ksuid())}")
workspace_example_one = w.create(w_ea)
logger.info(workspace_example_one)

w_ea = EntityAttribute(label=f"{str(ksuid())}")
workspace_example_two = w.create(w_ea)

all_wc = wc.all()
logger.info(all_wc)

all_w = w.all(workspace_category_one)
logger.info(all_w)

