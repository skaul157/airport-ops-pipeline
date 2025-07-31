// Example Terraform configuration to provision a minimal Azure data pipeline.
// This configuration is intentionally simplified and should be customised
// before deployment.  It creates a resource group, storage account,
// Event Hubs namespace and hub, Databricks workspace and Synapse workspace.

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">=3.0"
    }
  }

  required_version = ">= 1.5.0"
}

provider "azurerm" {
  features {}
}

// Variables (override these values when running terraform apply)
variable "location" {
  type    = string
  default = "eastus"
}

variable "resource_group_name" {
  type    = string
  default = "airport-pipeline-rg"
}

// Resource group
resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
}

// Storage account and Data Lake container
resource "azurerm_storage_account" "lake" {
  name                     = "airportlake${random_string.sa.result}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true // enable hierarchical namespace for ADLS Gen2
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.lake.name
  container_access_type = "private"
}

// Event Hubs namespace and hub
resource "random_string" "eh" {
  length  = 6
  special = false
  upper   = false
}

resource "random_string" "sa" {
  length  = 8
  special = false
  upper   = false
}

resource "azurerm_eventhub_namespace" "ns" {
  name                = "airportns${random_string.eh.result}"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  sku                 = "Basic"
  capacity            = 1
  tags = {
    environment = "dev"
  }
}

resource "azurerm_eventhub" "flight_events" {
  name                = "flight-events"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 4
  message_retention   = 1
}

// Databricks workspace
resource "azurerm_databricks_workspace" "dbw" {
  name                = "airport-dbw"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  sku                 = "premium"
}

// Synapse placeholder (requires additional configuration)
resource "azurerm_synapse_workspace" "synapse" {
  name                                 = "airport-synapse"
  resource_group_name                  = azurerm_resource_group.main.name
  location                             = var.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_container.bronze.id
  sql_administrator_login              = "sqladmin"
  sql_administrator_login_password     = "P@ssw0rd1234!"
  identity {
    type = "SystemAssigned"
  }
}

output "data_lake_endpoint" {
  value = azurerm_storage_account.lake.primary_blob_endpoint
}

output "event_hub_namespace" {
  value = azurerm_eventhub_namespace.ns.name
}