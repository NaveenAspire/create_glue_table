"""This module that is used for create table from table definition"""
import argparse
import json
from csv import DictReader
from datetime import datetime
import boto3


class CreateTable:
    """This is the class for creating tables in glue"""

    def __init__(self, bucket_name, mode="rename") -> None:
        """This is the init method for class of CreateTable"""
        self.glue_client = boto3.client("glue")
        self.s3_client = boto3.client("s3")
        self.bucket_name = bucket_name
        self.mode = mode

    def get_table_definition(self):
        """This method is used for get the table definition from s3"""
        with open("csv_file.csv", "r") as file:
            csv_reader = DictReader(file)
            for row in csv_reader:
                key = (
                    "glue/table/"
                    + row["database_name"]
                    + "/"
                    + row["table_name"]
                    + ".json"
                )
                table_definition_obj = self.s3_client.get_object(
                    Bucket=self.bucket_name, Key=key
                )
                table_definition_data = json.loads(
                    table_definition_obj["Body"].read().decode("utf-8")
                )
                table_definition_data["StorageDescriptor"]["Location"] = row["s3_path"]
                self.create_table(table_definition_data)

    def create_table(self, table_definition):
        """This method is used to create the tables from table definition"""
        try:
            key_list = [
                "Name",
                "Description",
                "Owner",
                "LastAccessTime",
                "LastAnalyzedTime",
                "Retention",
                "StorageDescriptor",
                "PartitionKeys",
                "ViewOriginalText",
                "ViewExpandedText",
                "TableType",
                "Parameters",
                "TargetTable",
            ]
            kwarg = {
                key: table_definition[key]
                for key in key_list
                if key in table_definition.keys()
            }
            self.glue_client.create_table(**kwarg)
        except self.glue_client.exceptions.AlreadyExistsException:
            if self.mode == "replace":
                self.glue_client.delete_table(
                    DatabaseName=table_definition["DatabaseName"],
                    TableName=table_definition["Name"],
                )
                self.create_table(table_definition)
            elif self.mode == "rename":
                self.rename_table(table_definition)
                self.create_table(table_definition)
            else:
                print('Invalid mode operation enter "rename" or "replace"')

    def rename_table(self, table_definition):
        """This is method used to rename the table for existing table name"""
        exist_table_response = self.glue_client.get_table(
            DatabaseName=table_definition["DatabaseName"], Name=table_definition["Name"]
        )

        table_name = (
            table_definition["Name"] + "_" + datetime.now().strftime("%Y%m%d_%H%M%S")
        )
        exist_table_response["Name"] = table_name
        self.create_table(exist_table_response)
        self.glue_client.delete_table(
            DatabaseName=table_definition["DatabaseName"],
            TableName=table_definition["Name"],
        )


def main():
    """This is the main method for module create table"""
    parser = argparse.ArgumentParser(
        description="This argparser used for getting bucket name and table creating mode"
    )
    parser.add_argument(
        "--bucket_name",
        type=str,
        help="Enter bucket name of table definition",
        required=True,
    )
    parser.add_argument(
        "--mode",
        type=str,
        help='Enter the mode for table name exist as "replace" or "rename" rename is default',
    )
    args = parser.parse_args()
    create_tables = CreateTable(args.bucket_name, mode=args.mode)
    create_tables.get_table_definition()


if __name__ == "__main__":
    main()
