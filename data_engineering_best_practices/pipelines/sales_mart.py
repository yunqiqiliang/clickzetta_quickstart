# ********************************************************************#
# author: qiliang
# create time: 2025-01-15 14:03:46
# ********************************************************************#
import subprocess
import sys

# Install necessary packages

# Force install the specific version of the package and overwrite the existing version
subprocess.run([sys.executable, "-m", "pip", "install", "clickzetta_zettapark_python", "--target", "/home/system_normal", "--force-reinstall", "-i", "https://pypi.tuna.tsinghua.edu.cn/simple"], stderr=subprocess.DEVNULL)
subprocess.run([sys.executable, "-m", "pip", "install", "faker", "--target", "/home/system_normal", "--force-reinstall", "-i", "https://pypi.tuna.tsinghua.edu.cn/simple"], stderr=subprocess.DEVNULL)


# Ensure the package path is added to sys.path
sys.path.append('/home/system_normal')

print("Package installed successfully and dependencies overwritten")

schema_name = ${schema}

########################################################################
# GENERATING FAKE BRONZE DATA !!!
########################################################################

from typing import Dict, List, Optional,Tuple
import logging,random
import os, json, uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional,Tuple
from clickzetta.zettapark.session import Session
from clickzetta.zettapark import DataFrame
from clickzetta.zettapark.functions import col, current_timestamp, expr, lit, to_date, count
from clickzetta.zettapark.types import (IntegerType, StringType, StructField,StructType, TimestampType)
from faker import Faker

def _get_orders(
    cust_ids: List[int], num_orders: int
) -> List[Tuple[str, int, str, str, datetime, datetime]]:
    items = [
        "chair",
        "car",
        "toy",
    ]
    return [
        (
            str(uuid.uuid4()),
            int(random.choice(cust_ids)),
            str(uuid.uuid4()),
            random.choice(items),
            datetime.now(),
            datetime.now(),
        )
        for _ in range(num_orders)
    ]


def _get_customer_data(
    cust_ids: List[int],
) -> List[Tuple[int, str, str, str, datetime, datetime]]:
    fake = Faker('zh_CN')
    return [
        (
            cust_id,
            fake.first_name(),
            fake.last_name(),
            fake.province(),
            datetime.now(),
            datetime.now(),
        )
        for cust_id in cust_ids
    ]


def generate_bronze_data(
    clickzetta: Session,
    iteration: int = 1,
    orders_bucket: str = "app-orders",
    **kwargs,
) -> List[DataFrame]:
    cust_ids = [i for i in range(1000)]
    return [
        clickzetta.createDataFrame(
            data=_get_customer_data(cust_ids),
            schema=StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("first_name", StringType(), True),
                    StructField("last_name", StringType(), True),
                    StructField("state_id", StringType(), True),
                    StructField("datetime_created", TimestampType(), True),
                    StructField("datetime_updated", TimestampType(), True),
                ]
            ),
        ),
        clickzetta.createDataFrame(
            data=_get_orders(cust_ids, 10000),
            schema=StructType(
                [
                    StructField("order_id", StringType(), True),
                    StructField("customer_id", IntegerType(), True),
                    StructField("item_id", StringType(), True),
                    StructField("item_name", StringType(), True),
                    StructField("delivered_on", TimestampType(), True),
                    StructField(
                        "datetime_order_placed", TimestampType(), True
                    ),
                ]
            ),
        ),
    ]


########################################################################

@dataclass
class DeltaDataSet:
    name: str
    curr_data: DataFrame
    primary_keys: List[str]
    storage_path: str
    table_name: str
    data_type: str
    schema: str
    partition: str
    skip_publish: bool = False
    replace_partition: bool = False


class InValidDataException(Exception):
    pass


class StandardETL(ABC):
    def __init__(
        self,
        storage_path: Optional[str] = None,
        schema: Optional[str] = None,
        partition: Optional[str] = None,
    ):
        self.STORAGE_PATH = storage_path or "s3a://adventureworks/delta"
        self.SCHEMA = schema or "data_engineering_best_practices"
        self.DEFAULT_PARTITION = partition or datetime.now().strftime(
            "%Y-%m-%d-%H-%M-%S"
        )

    def run_data_validations(self, input_datasets: Dict[str, DeltaDataSet]):
        context = gx.get_context(
            context_root_dir=os.path.join(
                os.getcwd(),
                "adventureworks",
                "great_expectations",
            )
        )

        print(f"Context root directory: {context.root_directory}")
        
        validations = []
        for input_dataset in input_datasets.values():
            validations.append(
                {
                    "batch_request": context.get_datasource("spark_datasource")
                    .get_asset(input_dataset.name)
                    .build_batch_request(dataframe=input_dataset.curr_data),
                    "expectation_suite_name": input_dataset.name,
                }
            )
        return context.run_checkpoint(
            checkpoint_name="dq_checkpoint", validations=validations
        ).list_validation_results()

    def validate_data(self, input_datasets: Dict[str, DeltaDataSet]) -> bool:
        results = {}
        for validation in self.run_data_validations(input_datasets):
            results[
                validation.get("meta").get("expectation_suite_name")
            ] = validation.get("success")
        for k, v in results.items():
            if not v:
                raise InValidDataException(
                    f"The {k} dataset did not pass validation, please check"
                    " the metadata db for more information"
                )

        return True

    def check_required_inputs(
        self, input_datasets: Dict[str, DeltaDataSet], required_ds: List[str]
    ) -> None:
        if not all([ds in input_datasets for ds in required_ds]):
            raise ValueError(
                f"The input_datasets {input_datasets.keys()} does not contain"
                f" {required_ds}"
            )

    def construct_join_string(self, keys: List[str]) -> str:
        return " AND ".join([f"target.{key} = source.{key}" for key in keys])

    def publish_data(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        clickzetta: Session,
        **kwargs,
    ) -> None:
        for input_dataset in input_datasets.values():
            if not input_dataset.skip_publish:
                curr_data = input_dataset.curr_data.withColumn(
                    "etl_inserted", current_timestamp()
                ).withColumn("partition", lit(input_dataset.partition))
                print(f"input_dataset is {input_dataset}")
                curr_data.show()
                curr_data.write.save_as_table(input_dataset.name, mode="append" , table_type="transient")

    @abstractmethod
    def get_bronze_datasets(
        self, clickzetta: Session, **kwargs
    ) -> Dict[str, DeltaDataSet]:
        pass

    @abstractmethod
    def get_silver_datasets(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        clickzetta: Session,
        **kwargs,
    ) -> Dict[str, DeltaDataSet]:
        pass

    @abstractmethod
    def get_gold_datasets(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        clickzetta: Session,
        **kwargs,
    ) -> Dict[str, DeltaDataSet]:
        pass

    def run(self, clickzetta: Session, **kwargs):
        partition = kwargs.get("partition")
        bronze_data_sets = self.get_bronze_datasets(clickzetta, partition=partition)
        # self.validate_data(bronze_data_sets)
        self.publish_data(bronze_data_sets, clickzetta)
        logging.info(
            "Created, validated & published bronze datasets:"
            f" {[ds for ds in bronze_data_sets.keys()]}"
        )

        silver_data_sets = self.get_silver_datasets(
            bronze_data_sets, clickzetta, partition=partition
        )
        # self.validate_data(silver_data_sets)
        self.publish_data(silver_data_sets, clickzetta)
        logging.info(
            "Created, validated & published silver datasets:"
            f" {[ds for ds in silver_data_sets.keys()]}"
        )

        gold_data_sets = self.get_gold_datasets(
            silver_data_sets, clickzetta, partition=partition
        )
        # self.validate_data(gold_data_sets)
        self.publish_data(gold_data_sets, clickzetta)
        logging.info(
            "Created, validated & published gold datasets:"
            f" {[ds for ds in gold_data_sets.keys()]}"
        )


class SalesMartETL(StandardETL):
    def get_bronze_datasets(
        self, clickzetta: Session, **kwargs
    ) -> Dict[str, DeltaDataSet]:
        customer_df, orders_df = generate_bronze_data(clickzetta)
        return {
            "customer": DeltaDataSet(
                name="customer",
                curr_data=customer_df,
                primary_keys=["id", "partition"],
                storage_path=f"{self.STORAGE_PATH}/customer",
                table_name="customer",
                data_type="delta",
                schema=f"{self.SCHEMA}",
                partition=kwargs.get("partition", self.DEFAULT_PARTITION),
                replace_partition=True,
            ),
            "orders": DeltaDataSet(
                name="orders",
                curr_data=orders_df,
                primary_keys=["order_id", "partition"],
                storage_path=f"{self.STORAGE_PATH}/orders",
                table_name="orders",
                data_type="delta",
                schema=f"{self.SCHEMA}",
                partition=kwargs.get("partition", self.DEFAULT_PARTITION),
                replace_partition=True,
            ),
        }
    
    def get_dim_customer(
        self, customer: DeltaDataSet, clickzetta: Session, **kwargs
    ) -> DataFrame:
        customer_df = customer.curr_data
        dim_customer = kwargs["dim_customer"]
        # generate pk
        customer_df = customer_df.withColumn(
            "customer_sur_id",
            expr("md5(concat(id, datetime_updated))"),
        )
        # get only latest customer rows in dim_customer
        # since dim_customer may have multiple rows per customer (SCD2)
        dim_customer_latest = dim_customer.where("current = true")

        # get net new rows to insert
        customer_df_insert_net_new = (
            customer_df.join(
                dim_customer_latest,
                (customer_df.id == dim_customer_latest.id)
                & (
                    dim_customer_latest.datetime_updated
                    < customer_df.datetime_updated
                ),
                "leftanti",
            )
            .select(
                customer_df.id.alias("id"),
                customer_df.customer_sur_id.alias("customer_sur_id"),
                customer_df.first_name.alias("first_name"),
                customer_df.last_name.alias("last_name"),
                customer_df.state_id.alias("state_id"),
                customer_df.datetime_created.alias("datetime_created"),
                customer_df.datetime_updated.alias("datetime_updated"),
            )
            .withColumn("current", lit(True))
            .withColumn("valid_from", customer_df.datetime_updated)
            .withColumn("valid_to", lit("2099-01-01 12:00:00.0000"))
        )

        # get rows to insert for existing ids
        customer_df_insert_existing_ids = (
            customer_df.join(
                dim_customer_latest,
                (customer_df.id == dim_customer_latest.id)
                & (
                    dim_customer_latest.datetime_updated
                    < customer_df.datetime_updated
                ),
            )
            .select(
                customer_df.id.alias("id"),
                customer_df.customer_sur_id.alias("customer_sur_id"),
                customer_df.first_name.alias("first_name"),
                customer_df.last_name.alias("last_name"),
                customer_df.state_id.alias("state_id"),
                customer_df.datetime_created.alias("datetime_created"),
                customer_df.datetime_updated.alias("datetime_updated"),
            )
            .withColumn("current", lit(True))
            .withColumn("valid_from", customer_df.datetime_updated)
            .withColumn("valid_to", lit("2099-01-01 12:00:00.0000"))
        )

        # get rows to be updated
        customer_df_ids_update = (
            dim_customer_latest.join(
                customer_df,
                (dim_customer_latest.id == customer_df.id)
                & (
                    dim_customer_latest.datetime_updated
                    < customer_df.datetime_updated
                ),
            )
            .select(
                dim_customer_latest.id.alias("id"),
                dim_customer_latest.customer_sur_id.alias("customer_sur_id"),
                dim_customer_latest.first_name.alias("first_name"),
                dim_customer_latest.last_name.alias("last_name"),
                dim_customer_latest.state_id.alias("state_id"),
                dim_customer_latest.datetime_created.alias("datetime_created"),
                dim_customer_latest.datetime_updated.alias("datetime_updated"),
                dim_customer_latest.valid_from.alias("valid_from"),
                customer_df.datetime_updated.alias("valid_to")
            )
            .withColumn("current", lit(False))
        )

        return customer_df_insert_net_new.unionByName(
            customer_df_insert_existing_ids
        ).unionByName(customer_df_ids_update)

    def get_fct_orders(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        clickzetta: Session,
        **kwargs,
    ) -> DataFrame:
        dim_customer = input_datasets["dim_customer"].curr_data
        orders_df = input_datasets["orders"].curr_data

        dim_customer_curr_df = dim_customer.where("current = true")
        return orders_df.join(
            dim_customer_curr_df,
            orders_df.customer_id == dim_customer_curr_df.id,
            "left",
        ).select(
            orders_df.order_id,
            orders_df.customer_id,
            orders_df.item_id,
            orders_df.item_name,
            orders_df.delivered_on,
            orders_df.datetime_order_placed,
            dim_customer_curr_df.customer_sur_id,
        )

    def get_silver_datasets(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        clickzetta: Session,
        **kwargs,
    ) -> Dict[str, DeltaDataSet]:
        self.check_required_inputs(input_datasets, ["customer", "orders"])
        dim_customer_df = self.get_dim_customer(
            input_datasets["customer"],
            clickzetta,
            dim_customer=clickzetta.read.table(f"{self.SCHEMA}.dim_customer"),
        )

        silver_datasets = {}
        silver_datasets["dim_customer"] = DeltaDataSet(
            name="dim_customer",
            curr_data=dim_customer_df,
            primary_keys=["customer_sur_id"],
            storage_path=f"{self.STORAGE_PATH}/dim_customer",
            table_name="dim_customer",
            data_type="delta",
            schema=f"{self.SCHEMA}",
            partition=kwargs.get("partition", self.DEFAULT_PARTITION),
        )
        self.publish_data(silver_datasets, clickzetta)
        silver_datasets["dim_customer"].curr_data = clickzetta.read.table(
            f"{self.SCHEMA}.dim_customer"
        )
        silver_datasets["dim_customer"].skip_publish = True
        input_datasets["dim_customer"] = silver_datasets["dim_customer"]

        silver_datasets["fct_orders"] = DeltaDataSet(
            name="fct_orders",
            curr_data=self.get_fct_orders(input_datasets, clickzetta),
            primary_keys=["order_id"],
            storage_path=f"{self.STORAGE_PATH}/fct_orders",
            table_name="fct_orders",
            data_type="delta",
            schema=f"{self.SCHEMA}",
            partition=kwargs.get("partition", self.DEFAULT_PARTITION),
            replace_partition=True,
        )
        return silver_datasets


    def get_sales_mart(self, input_datasets: dict, **kwargs) -> DataFrame:
        dim_customer = (
            input_datasets["dim_customer"].curr_data
            .where(col("current") == True)
            .select("customer_sur_id", "state_id")
        )

        fct_orders = input_datasets["fct_orders"].curr_data
            
        # 去除时区信息并转换delivered_on为日期类型
        fct_orders = fct_orders.withColumn("deliver_date", to_date(col("delivered_on").substr(1, 10), "yyyy-MM-dd"))
        
        return (
            fct_orders
            .join(dim_customer, fct_orders["customer_sur_id"] == dim_customer["customer_sur_id"], "left")
            .select("deliver_date", "state_id")
            .groupBy("deliver_date", "state_id")
            .agg(count("*").alias("num_orders"))
        )


    def get_gold_datasets(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        clickzetta: Session,
        **kwargs,
    ) -> Dict[str, DeltaDataSet]:
        self.check_required_inputs(
            input_datasets, ["dim_customer", "fct_orders"]
        )
        sales_mart_df = self.get_sales_mart(input_datasets)
        return {
            "sales_mart": DeltaDataSet(
                name="sales_mart",
                curr_data=sales_mart_df,
                primary_keys=["deliver_date", "state_id", "partition"],
                storage_path=f"{self.STORAGE_PATH}/sales_mart",
                table_name="sales_mart",
                data_type="delta",
                schema=f"{self.SCHEMA}",
                partition=kwargs.get("partition", self.DEFAULT_PARTITION),
                replace_partition=True,
            )
        }


if __name__ == '__main__':
    
    hints = dict()
    hints['sdk.job.timeout'] = 3
    hints['query_tag'] = 'test_conn_hints_zettapark'
    connection_parameters = {
        "service": ${service},
        "instance": ${instance},
        "workspace": ${workspace},
        "schema": ${schema},
        "vcluster": ${vcluster},
        "username": ${username},
        "password": ${password},
        "sdk_job_timeout": 10,
        "hints": hints,
    }

    
    
    

    clickzetta = Session.builder.configs(connection_parameters).create()
    
    print("连接到Lakehouse成功！...\n")

    print("正在加载数据到Lakehouse并进行转换.....\n")
    sm = SalesMartETL()
    partition = datetime.now().strftime(
        "%Y-%m-%d-%H-%M"
    )  # usually from orchestrator -%S
    sm.run(clickzetta, partition=partition)
    print("数据加载并转换完成！...\n")
    clickzetta.close()
