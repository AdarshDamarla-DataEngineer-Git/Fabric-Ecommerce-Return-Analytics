# Microsoft Fabric CI/CD setup

This repository validates pull requests from `dev` to `main`. Production deployment is never automatic: it can only be requested manually from the **Fabric CI/CD** workflow on `main`, requires the exact confirmation text `DEPLOY`, and is then paused by the protected `production` GitHub environment for reviewer approval.

## 1. Create and protect the production workspace

Create a Fabric workspace that is separate from the development workspace. Assign it to Fabric capacity. Do not connect the production workspace to the development branch.

Create a Microsoft Entra application/service principal for this repository. Add it to the production Fabric workspace with **Contributor** access (use Admin only if an item type or operation demonstrably requires it). In the Fabric admin portal, allow the security group containing this service principal to use Fabric APIs.

The workflow uses OpenID Connect (OIDC), so there is no long-lived client secret. Add a federated identity credential to the Entra application with:

- Issuer: `https://token.actions.githubusercontent.com`
- Subject: `repo:AdarshDamarla-DataEngineer-Git/Fabric-Ecommerce-Return-Analytics:environment:production`
- Audience: `api://AzureADTokenExchange`

No Azure subscription role is needed solely to call Fabric APIs.

## 2. Configure the GitHub production environment

In **Repository settings > Environments**, create `production` and configure:

- Required reviewers (at least one person other than the requester is recommended).
- Prevent self-review, if available for the repository plan.
- Deployment branches restricted to `main`.

Add these environment variables:

| Name | Purpose |
| --- | --- |
| `AZURE_CLIENT_ID` | Application/client ID of the Entra app. |
| `AZURE_TENANT_ID` | Entra directory/tenant ID. |
| `PROD_WORKSPACE_ID` | ID of the separate production Fabric workspace. |
| `PROD_WAREHOUSE_ENDPOINT` | Production warehouse SQL endpoint used by the data pipeline. |

Add this environment secret:

| Name | Purpose |
| --- | --- |
| `PROD_SOURCE_CONNECTION_ID` | Fabric connection ID for the production pipeline's HTTP source. Treat it as sensitive metadata. |

The workflow receives only an OIDC token and `contents: read`; the production job alone receives `id-token: write`.

## 3. Protect `main`

Create a branch ruleset for `main` that:

- Requires pull requests.
- Requires the `validate-dev-to-main` status check.
- Requires approvals and dismissal of stale approvals.
- Blocks force pushes and deletion.
- Optionally requires signed commits and code-owner review.

The workflow intentionally fails PR validation when the source is not `dev` or the destination is not `main`.

## 4. Deploy only after approval

After an approved `dev` to `main` PR is merged:

1. Open **Actions > Fabric CI/CD > Run workflow**.
2. Select `main`.
3. Enter `DEPLOY`.
4. Review the pending `production` environment deployment and approve it only after confirming the target workspace and parameter values.

The deployment publishes Lakehouse, Warehouse, Notebook, and Data Pipeline definitions using Microsoft's `fabric-cicd` library. It does **not** remove orphaned production items.

## Validation scope and limitations

- PR checks are offline structural checks: valid JSON, valid Fabric item metadata, unique logical IDs, supported item types, and expected content files. They do not execute notebooks, SQL, or pipelines.
- Lakehouse Git/deployment contains metadata, not table/file data. Production data must be loaded separately.
- The external HTTP connection must already exist in Fabric and be usable by the deployed pipeline. Credentials are managed in Fabric, not in GitHub.
- Environment-specific IDs and the warehouse endpoint are replaced through `parameter.yml`. Review this file whenever new cross-item or external connections are added.
- Fabric item support and REST behavior vary by item type. A first deployment should target an empty non-production rehearsal workspace before approving production.
- Deployments are not transactional and may partially complete. Inspect the job log and production workspace after a failure before retrying.
- Private Link, outbound access protection, tenant policy, capacity state, and unsupported item features can block deployment even when CI passes.

