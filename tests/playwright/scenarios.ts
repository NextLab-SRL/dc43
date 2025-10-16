export interface SetupWizardScenario {
  readonly description: string;
  readonly moduleSelections: Record<string, string>;
  readonly configurationOverrides: Record<string, string>;
  readonly tags?: readonly string[];
}

export const setupWizardScenarios: Record<string, SetupWizardScenario> = {
  local_only: {
    description: 'Select embedded defaults and filesystem stores for local demos.',
    tags: ['happy_path'],
    moduleSelections: {
      contracts_backend: 'filesystem',
      products_backend: 'filesystem',
      governance_store: 'embedded_memory',
      data_quality: 'embedded_engine',
      governance_service: 'embedded_monolith',
      governance_deployment: 'local_python',
      governance_extensions: 'none',
      pipeline_integration: 'spark',
      user_interface: 'local_web',
      ui_deployment: 'skip_hosting',
      demo_automation: 'skip_demo',
      authentication: 'none',
    },
    configurationOverrides: {
      config__contracts_backend__work_dir: '/workspace',
      config__contracts_backend__contracts_dir: '/workspace/contracts',
      config__products_backend__products_dir: '/workspace/products',
    },
  },
  databricks_jobs: {
    description: 'Target Databricks Unity Catalog integrations with filesystem backends.',
    tags: ['governance_focus', 'databricks'],
    moduleSelections: {
      contracts_backend: 'filesystem',
      products_backend: 'filesystem',
      governance_store: 'embedded_memory',
      data_quality: 'embedded_engine',
      governance_service: 'embedded_monolith',
      governance_deployment: 'local_python',
      governance_extensions: 'unity_catalog',
      pipeline_integration: 'spark',
      user_interface: 'local_web',
      ui_deployment: 'skip_hosting',
      demo_automation: 'skip_demo',
      authentication: 'none',
    },
    configurationOverrides: {
      config__contracts_backend__work_dir: '/tmp/contracts',
      config__contracts_backend__contracts_dir: '/tmp/contracts/specs',
      config__products_backend__products_dir: '/tmp/contracts/products',
      config__governance_extensions__workspace_url:
        'https://adb-1234567890123456.7.azuredatabricks.net',
      config__governance_extensions__catalog: 'main',
      config__governance_extensions__schema: 'contracts',
      config__governance_extensions__token: '{{DATABRICKS_UC_TOKEN}}',
      config__governance_extensions__dataset_prefix: 'table://',
    },
  },
  enterprise_oidc: {
    description:
      'Collibra-backed governance with Unity Catalog hooks, OIDC auth, and AWS Terraform deployment stubs.',
    tags: ['enterprise'],
    moduleSelections: {
      contracts_backend: 'collibra',
      products_backend: 'collibra',
      governance_store: 'delta_lake',
      data_quality: 'remote_http',
      governance_service: 'remote_api',
      governance_deployment: 'aws_terraform',
      governance_extensions: 'unity_catalog',
      pipeline_integration: 'spark',
      user_interface: 'remote_portal',
      ui_deployment: 'aws_terraform',
      demo_automation: 'skip_demo',
      authentication: 'oauth_oidc',
    },
    configurationOverrides: {
      config__contracts_backend__base_url: 'https://collibra.acme.example',
      config__contracts_backend__client_id: 'contracts-service',
      config__contracts_backend__client_secret: '{{COLLIBRA_CONTRACTS_SECRET}}',
      config__contracts_backend__domain_id: 'DATA_CONTRACTS',
      config__products_backend__base_url: 'https://collibra.acme.example',
      config__products_backend__client_id: 'products-service',
      config__products_backend__client_secret: '{{COLLIBRA_PRODUCTS_SECRET}}',
      config__products_backend__domain_id: 'DATA_PRODUCTS',
      config__governance_store__status_table: 'main.governance.dq_status',
      config__governance_store__activity_table: 'main.governance.dq_activity',
      config__governance_store__link_table:
        'main.governance.dq_dataset_contract_links',
      config__governance_store__workspace_url:
        'https://adb-1234567890123456.7.azuredatabricks.net',
      config__governance_store__workspace_token: '{{DATABRICKS_GOVERNANCE_TOKEN}}',
      config__data_quality__base_url: 'https://quality.acme.example',
      config__data_quality__api_token: '{{QUALITY_API_TOKEN}}',
      config__data_quality__token_scheme: 'Bearer',
      config__data_quality__extra_headers: 'X-Team=governance',
      config__governance_service__base_url:
        'https://governance.acme.example',
      config__governance_service__api_token: '{{GOVERNANCE_API_TOKEN}}',
      config__governance_deployment__aws_region: 'us-east-1',
      config__governance_deployment__cluster_name: 'dc43-governance',
      config__governance_deployment__ecr_image_uri:
        '123456789012.dkr.ecr.us-east-1.amazonaws.com/dc43-backends:prod',
      config__governance_deployment__private_subnet_ids: 'subnet-aaa,subnet-bbb',
      config__governance_deployment__load_balancer_subnet_ids:
        'subnet-ccc,subnet-ddd',
      config__governance_deployment__service_security_group_id:
        'sg-0123456789abcdef0',
      config__governance_deployment__load_balancer_security_group_id:
        'sg-abcdef0123456789',
      config__governance_deployment__certificate_arn:
        'arn:aws:acm:us-east-1:123456789012:certificate/abcdef01',
      config__governance_deployment__vpc_id: 'vpc-0abc123def4567890',
      config__governance_deployment__contract_store_mode: 'sql',
      config__governance_deployment__contract_store_dsn_secret_arn:
        'arn:aws:secretsmanager:us-east-1:123456789012:secret:contracts-dsn',
      config__governance_extensions__workspace_url:
        'https://adb-1234567890123456.7.azuredatabricks.net',
      config__governance_extensions__catalog: 'main',
      config__governance_extensions__schema: 'contracts',
      config__governance_extensions__token: '{{DATABRICKS_UC_TOKEN}}',
      config__governance_extensions__dataset_prefix: 'table://',
      config__pipeline_integration__runtime: 'databricks job',
      config__pipeline_integration__workspace_url:
        'https://adb-1234567890123456.7.azuredatabricks.net',
      config__pipeline_integration__workspace_profile: 'governance-jobs',
      config__pipeline_integration__cluster_reference: 'job:dc43-governance',
      config__user_interface__portal_url: 'https://contracts.acme.example',
      config__ui_deployment__aws_region: 'us-east-1',
      config__ui_deployment__cluster_name: 'dc43-ui',
      config__ui_deployment__ecr_image_uri:
        '123456789012.dkr.ecr.us-east-1.amazonaws.com/dc43-ui:prod',
      config__ui_deployment__private_subnet_ids: 'subnet-aaa,subnet-bbb',
      config__ui_deployment__load_balancer_subnet_ids:
        'subnet-ccc,subnet-ddd',
      config__ui_deployment__service_security_group_id:
        'sg-fedcba9876543210',
      config__ui_deployment__load_balancer_security_group_id:
        'sg-1234567890abcdef',
      config__ui_deployment__certificate_arn:
        'arn:aws:acm:us-east-1:123456789012:certificate/fedcba10',
      config__ui_deployment__vpc_id: 'vpc-0abc123def4567890',
      config__authentication__issuer_url:
        'https://login.microsoftonline.com/contoso/v2.0',
      config__authentication__client_id: '00000000-0000-0000-0000-000000000000',
      config__authentication__client_secret: '{{OIDC_CLIENT_SECRET}}',
      config__authentication__redirect_uri:
        'https://contracts.acme.example/oauth/callback',
    },
  },
};

export const setupWizardScenarioEntries = Object.entries(setupWizardScenarios);
