// Core infrastructure for one environment (dev/test/prod). Deploy per-env with distinct
// parameter files (infra/arm_bicep/params.<env>.json). This provisions the PHI-handling
// backbone: ADLS Gen2 with private endpoint, Key Vault, Databricks workspace (VNet-injected),
// and an ADF instance for vendor-source ingestion orchestration.
//
// NOTE: this is illustrative scaffolding, not a turnkey deploy — VNet/subnet IDs, DNS zones,
// and RBAC role assignments must be filled in for your subscription before `az deployment
// group create`.

@allowed(['dev', 'test', 'prod'])
param environmentName string
param location string = resourceGroup().location
param vnetId string
param privateEndpointSubnetId string
param databricksSubnetPublicId string
param databricksSubnetPrivateId string

var namePrefix = 'sleepapnea${environmentName}'

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: '${namePrefix}sa'
  location: location
  sku: { name: 'Standard_GRS' }   // geo-redundant: HIPAA-grade DR posture
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true             // hierarchical namespace = ADLS Gen2
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    networkAcls: {
      defaultAction: 'Deny'
      bypass: 'AzureServices'
    }
    encryption: {
      services: { blob: { enabled: true }, file: { enabled: true } }
      keySource: 'Microsoft.Storage'   // swap to Microsoft.Keyvault + CMK for stricter prod posture
    }
  }
}

resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = [
  for c in ['raw', 'bronze', 'silver', 'gold', 'checkpoints', 'quarantine']: {
    name: '${storageAccount.name}/default/${c}'
  }
]

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: '${namePrefix}-kv'
  location: location
  properties: {
    tenantId: subscription().tenantId
    sku: { family: 'A', name: 'standard' }
    enableRbacAuthorization: true
    enablePurgeProtection: true
    enableSoftDelete: true
    networkAcls: { defaultAction: 'Deny', bypass: 'AzureServices' }
  }
}

resource storagePrivateEndpoint 'Microsoft.Network/privateEndpoints@2023-09-01' = {
  name: '${namePrefix}-sa-pe'
  location: location
  properties: {
    subnet: { id: privateEndpointSubnetId }
    privateLinkServiceConnections: [
      {
        name: '${namePrefix}-sa-plsc'
        properties: {
          privateLinkServiceId: storageAccount.id
          groupIds: ['dfs']
        }
      }
    ]
  }
}

resource databricksWorkspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: '${namePrefix}-dbx'
  location: location
  sku: { name: 'premium' }   // required for Unity Catalog, audit logs, IP access lists, RBAC
  properties: {
    managedResourceGroupId: subscriptionResourceId(
      'Microsoft.Resources/resourceGroups',
      '${namePrefix}-dbx-managed-rg'
    )
    parameters: {
      enableNoPublicIp: { value: true }       // no public IPs on cluster nodes (NPIP)
      customVirtualNetworkId: { value: vnetId }
      customPublicSubnetName: { value: databricksSubnetPublicId }
      customPrivateSubnetName: { value: databricksSubnetPrivateId }
    }
  }
}

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: '${namePrefix}-adf'
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {
    publicNetworkAccess: 'Disabled'
  }
}

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: '${namePrefix}-law'
  location: location
  properties: {
    retentionInDays: environmentName == 'prod' ? 2555 : 90  // ~7yr prod retention for HIPAA audit logs
    sku: { name: 'PerGB2018' }
  }
}

output storageAccountName string = storageAccount.name
output keyVaultName string = keyVault.name
output databricksWorkspaceUrl string = databricksWorkspace.properties.workspaceUrl
output logAnalyticsWorkspaceId string = logAnalytics.id
