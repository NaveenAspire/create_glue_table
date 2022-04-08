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
            response = self.glue_client.create_table(
                CatalogId=table_definition.get("CatalogId"),
                DatabaseName=table_definition["DatabaseName"],
                TableInput={
                    "Name": table_definition["Name"],
                    "Description": table_definition.get("Description"),
                    "Owner": table_definition.get("Owner"),
                    "LastAccessTime": table_definition.get("LastAccessTime"),
                    "LastAnalyzedTime": table_definition.get("LastAnalyzedTime"),
                    "Retention": table_definition.get("Retention"),
                    "StorageDescriptor": {
                        "Columns": table_definition["StorageDescriptor"].get("Columns"),
                        "Location": table_definition["StorageDescriptor"].get(
                            "Location"
                        ),
                        "AdditionalLocations": table_definition[
                            "StorageDescriptor"
                        ].get("AdditionalLocations"),
                        "InputFormat": table_definition["StorageDescriptor"].get(
                            "InputFormat"
                        ),
                        "OutputFormat": table_definition["StorageDescriptor"].get(
                            "OutputFormat"
                        ),
                        "Compressed": table_definition["StorageDescriptor"].get(
                            "Compresed"
                        ),
                        "NumberOfBuckets": table_definition["StorageDescriptor"].get(
                            "NumberOfBuckets"
                        ),
                        "SerdeInfo": table_definition["StorageDescriptor"].get(
                            "SerdeInfo"
                        ),
                        "BucketColumns": table_definition["StorageDescriptor"].get(
                            "BucketColumns"
                        ),
                        "SortColumns": table_definition["StorageDescriptor"].get(
                            "SortColumns"
                        ),
                        "Parameters": table_definition["StorageDescriptor"].get(
                            "Parameters"
                        ),
                        "SkewedInfo": table_definition["StorageDescriptor"].get(
                            "SkewedInfo"
                        ),
                        "StoredAsSubDirectories": table_definition[
                            "StorageDescriptor"
                        ].get("StoredAsSubDirectories"),
                        "SchemaReference": table_definition["StorageDescriptor"].get(
                            "SchemaReference"
                        ),
                    },
                    "PartitionKeys": table_definition.get("PartitionKeys"),
                    "ViewOriginalText": table_definition.get("ViewOriginalText"),
                    "ViewExpandedText": table_definition.get("ViewExpandedText"),
                    "TableType": table_definition.get("TableType"),
                    "Parameters": table_definition.get("Parameters"),
                    "TargetTable": table_definition.get("TargetTable"),
                },
            )
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
        exist_table_response = self.glue_client.get_table(DatabaseName=table_definition['DatabaseName']
                                                          ,Name=table_definition['Name'])
        
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
