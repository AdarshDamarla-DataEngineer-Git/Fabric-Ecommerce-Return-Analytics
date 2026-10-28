# Warehouse Setup Scripts

T-SQL scripts that build the `Ecommerce_Returns_Warehouse` objects the pipeline needs. They mirror the definitions tracked by Fabric Git integration in `Fabric_Ecommerce_Return_Analysis/Ecommerce_Returns_Warehouse.Warehouse`, and are meant for setting up a new warehouse or rebuilding one by hand.

## Run order (new warehouse)

Open the Warehouse **SQL query editor** and run each script, in this order:

| Script | What it does | Re-runnable |
|---|---|---|
| `01_Create_Schemas.sql` | Creates `Returns_Control`, `Returns_Staging`, `Returns_Gold` | Errors with "already exists" if repeated (safe to ignore) |
| `02_Create_Control_Tables.sql` | `Pipeline_Watermark`, `Pipeline_Metadata` | Yes |
| `03_Create_Staging_Tables.sql` | Six `*_stg` tables | Yes |
| `04_Create_Gold_Tables.sql` | `Fact_Orders` and five dimensions | Yes |
| `05_Seed_Watermark.sql` | First watermark row (set the month **before** the first month to load) | Yes, never inserts a second row |
| `06_SP_Update_Watermark.sql` | Moves the watermark forward only | Yes (`CREATE OR ALTER`) |
| `07_SP_Insert_Pipeline_Metadata.sql` | Writes one audit row per run | Yes (`CREATE OR ALTER`) |
| `08_SP_Merge_Returns_Gold_Final.sql` | Merges staging into Gold in one transaction | Yes (`CREATE OR ALTER`) |

Utilities, run when needed:

| Script | Purpose |
|---|---|
| `09_Check_Run_History.sql` | Read-only: latest runs and the current watermark |
| `99_Reset_All_Tables.sql` | **Destructive.** Empties Gold, staging and control tables, then re-seeds the watermark. Development use only |

## Notes

- Run `01` before anything else. Tables and procedures depend on the schemas existing.
- The scripts do not touch the Lakehouse. Bronze, Silver and quarantine files stay as they are, even after `99_Reset_All_Tables.sql`.
- If you use Fabric Git integration, the warehouse objects in the `Ecommerce_Returns_Warehouse.Warehouse` folder are the source of truth for the deployed warehouse. Keep these scripts in sync with them.
- `Insert_Pipeline_Metadata` accepts error text up to 8,000 characters and stores the first 2,000, so a long Spark error cannot make the logging step fail.
