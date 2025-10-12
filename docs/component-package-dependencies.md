# Internal package dependency graph

The dc43 repository bundles multiple installable distributions that depend on
one another. The diagram below is generated from the package metadata to help
spot cycles and optional extras when adjusting dependencies.

To regenerate the diagram, run:

```bash
python scripts/dependency_graph.py --output docs/component-package-dependencies.mmd
```

and embed the resulting Mermaid snippet into this document.

```mermaid
graph TD
    dc43["dc43"]
    dc43_demo_app["dc43-demo-app"]
    dc43_service_clients["dc43-service-clients"]
    dc43_service_backends["dc43-service-backends"]
    dc43_integrations["dc43-integrations"]
    dc43_contracts_app["dc43-contracts-app"]
    dc43 --> dc43_contracts_app
    dc43 --> dc43_integrations
    dc43 --> dc43_service_backends
    dc43 --> dc43_service_clients
    dc43_demo_app --> dc43_contracts_app
    dc43_demo_app --> dc43_integrations
    dc43_demo_app --> dc43_service_backends
    dc43_demo_app --> dc43_service_clients
    dc43_service_backends --> dc43
    dc43_service_backends --> dc43_service_clients
    dc43_integrations --> dc43_service_clients
    dc43_contracts_app --> dc43_service_backends
    dc43_contracts_app --> dc43_service_clients
    dc43 -.-> dc43_demo_app
    dc43_integrations -.-> dc43_service_backends
    dc43_contracts_app -.-> dc43_integrations
    %% Dashed arrows indicate optional extras
```
