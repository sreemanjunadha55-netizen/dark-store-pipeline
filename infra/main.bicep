@description('Short name used in resource names')
param project string = 'darkstore'

@description('Azure region')
param location string = resourceGroup().location

@description('Globally unique storage account name (3-24 lowercase letters and digits)')
param storageAccountName string = 'stdarkstore55'

@description('Globally unique key vault name (3-24 characters)')
param keyVaultName string = 'kv-darkstore-46'

@description('Name of the managed resource group Databricks creates for its machines')
param databricksManagedRgName string = 'databricks-rg-dbw-darkstore-dev-sq7p763kubshs'

var containerNames = [
  'landing'
  'bronze'
  'silver'
  'gold'
]

// Built-in role: Storage Blob Data Contributor
var storageBlobDataContributor = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    accessTier: 'Hot'
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = [for name in containerNames: {
  parent: blobService
  name: name
  properties: {
    publicAccess: 'None'
  }
}]

resource connector 'Microsoft.Databricks/accessConnectors@2024-05-01' = {
  name: 'ac-${project}-dev'
  location: location
  identity: {
    type: 'SystemAssigned'
  }
}

resource connectorCanUseStorage 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, connector.id, storageBlobDataContributor)
  scope: storage
  properties: {
    principalId: connector.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributor)
  }
}

resource workspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: 'dbw-${project}-dev'
  location: location
  sku: {
    name: 'premium'
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', databricksManagedRgName)
  }
}

resource vault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    tenantId: subscription().tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    enableRbacAuthorization: false
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
  }
}

output storageDfsEndpoint string = storage.properties.primaryEndpoints.dfs
output workspaceUrl string = workspace.properties.workspaceUrl
