# Email Notifications (Web Activity)

The pipeline sends one email at the end of every run: **`[SUCCEEDED]`** with row counts, or **`[FAILED]`** with the error. It does this without Microsoft 365 or the Office 365 Outlook activity. A Fabric **Web activity** posts the details to a small **Google Apps Script** web app, which sends the mail from a Gmail account.

```
Pipeline (If_Check_Error)
   ├─ error_message set → Send_Failure_Email → Fail_Pipeline
   └─ no error         → Send_Success_Email
              │ HTTPS POST (plain text)
              ▼
   Google Apps Script web app  →  MailApp.sendEmail  →  your inbox
```

## Contents

1. [Prerequisites](#1-prerequisites)
2. [Create the Apps Script](#2-create-the-apps-script)
3. [Create the Fabric connection](#3-create-the-fabric-connection)
4. [Add the failure email activity](#4-add-the-failure-email-activity)
5. [Add the success email activity](#5-add-the-success-email-activity)
6. [Test](#6-test)
7. [Gmail filters](#7-gmail-filters)
8. [Security and limits](#8-security-and-limits)
9. [Troubleshooting](#9-troubleshooting)

## 1. Prerequisites

- A Google account to send from.
- Edit access to the pipeline in Fabric.
- The pipeline already contains the `If_Check_Error` activity. Its **True** branch holds `Fail_Pipeline`, and its **False** branch is empty until you add the success email.
- `If_Check_Error` should follow `Stored_Procedure_Insert_Metadata` on **Completed** (the blue arrow), so the email is still sent if the logging step itself fails.

## 2. Create the Apps Script

1. Go to [script.google.com](https://script.google.com) and click **New project**.
2. Replace the default code with:

   ```javascript
   // Deploy as Web app (Execute as: Me, Who has access: Anyone)
   // Handles failure and success emails. The pipeline sends a "Status:" line;
   // if it is missing, the mail is treated as FAILED.
   const TO = 'your.email@gmail.com';   // change this

   function doPost(e) {
     const text = (e && e.postData && e.postData.contents) || 'No details received';
     const pipeline = (text.match(/^Pipeline: (.*)$/m) || [])[1] || 'Fabric pipeline';
     const status = ((text.match(/^Status: (.*)$/m) || [])[1] || 'FAILED').trim();
     MailApp.sendEmail({
       to: TO,
       subject: '[' + status + '] ' + pipeline,
       body: text
     });
     return ContentService.createTextOutput('sent');
   }
   ```

3. Set `TO` to the address that should receive the alerts.

   The same script is available as a file: [`apps_script/Code.gs`](../apps_script/Code.gs). Copy its contents if you prefer that to the block above.

4. Click **Deploy → New deployment → Web app**.
   - Execute as: **Me**
   - Who has access: **Anyone**
5. Click **Deploy**, authorize with your Google account, and copy the **Web app URL** (it ends in `/exec`).

To change the script later, use **Deploy → Manage deployments → pencil → Version: New version → Deploy**. Editing the existing deployment keeps the same URL; creating a new deployment gives a new URL.

## 3. Create the Fabric connection

1. In Fabric: **Settings (gear) → Manage connections and gateways → New**.
2. Choose **Cloud** (not a virtual network or on-premises gateway; the script is a public URL).
3. Connection type: **Web v2**.
4. Connection name: for example `GAS_Failure_Email_Webhook`.
5. Base URL: the `/exec` URL from step 2.
6. Authentication: **Anonymous**.
7. Save.

## 4. Add the failure email activity

In the pipeline, open `If_Check_Error` and go into the **True** branch.

1. Add a **Web** activity named `Send_Failure_Email` before `Fail_Pipeline`.
2. Settings:

   | Setting | Value |
   |---|---|
   | Connection | `GAS_Failure_Email_Webhook` |
   | Relative URL | empty |
   | Method | `POST` |
   | Headers | `Content-Type` = `text/plain` |
   | Body | the expression below |

3. Body (**Add dynamic content**):

   ```
   @concat('Status: FAILED', decodeUriComponent('%0A'), 'Pipeline: ', pipeline().PipelineName, decodeUriComponent('%0A'), 'Run ID: ', pipeline().RunId, decodeUriComponent('%0A'), 'Trigger: ', pipeline().TriggerType, decodeUriComponent('%0A'), 'Trigger time: ', pipeline().TriggerTime, decodeUriComponent('%0A'), 'Processing: ', variables('watermark_year'), '-', variables('watermark_month_name'), decodeUriComponent('%0A'), decodeUriComponent('%0A'), 'Error: ', variables('error_message'))
   ```

4. Connect `Send_Failure_Email` to `Fail_Pipeline` with the **Completed** (blue) arrow, so the pipeline still fails with the real error even if the email could not be sent.
5. Use the shortest timeout the Fabric portal allows (the minimum is 10 minutes) and 1 retry, so a problem with the script cannot hold the pipeline for long.
6. Save.

`decodeUriComponent('%0A')` inserts a line break. The body is plain text, so no JSON escaping of the error message is needed.

## 5. Add the success email activity

Open the **False** branch of `If_Check_Error` (it runs when no stage failed).

1. Add a **Web** activity named `Send_Success_Email` with the same connection, empty relative URL, method `POST` and header `Content-Type` = `text/plain`.
2. Body (**Add dynamic content**):

   ```
   @concat('Status: SUCCEEDED', decodeUriComponent('%0A'), 'Pipeline: ', pipeline().PipelineName, decodeUriComponent('%0A'), 'Run ID: ', pipeline().RunId, decodeUriComponent('%0A'), 'Trigger: ', pipeline().TriggerType, decodeUriComponent('%0A'), 'Trigger time: ', pipeline().TriggerTime, decodeUriComponent('%0A'), 'Processed: ', variables('watermark_year'), '-', variables('watermark_month_name'), decodeUriComponent('%0A'), decodeUriComponent('%0A'), 'Bronze records: ', string(variables('bronze_record_processed')), decodeUriComponent('%0A'), 'Silver records: ', string(variables('silver_record_processed')), decodeUriComponent('%0A'), 'Gold fact (staging) records: ', string(variables('gold_fact_Stg_record_processed')))
   ```

3. Save.

## 6. Test

- **Failure:** run the pipeline with `reprocess_year = 9999` and `reprocess_month = 1`. A stage fails and you should get `[FAILED] Ecommerce_Returns_Pipeline` with the error text. Clear the parameters afterwards.
- **Success:** run the pipeline normally. You should get `[SUCCEEDED] Ecommerce_Returns_Pipeline` with the processed month and the Bronze, Silver and Gold-staging counts.

Check spam the first time.

Example success email:

```
Status: SUCCEEDED
Pipeline: Ecommerce_Returns_Pipeline
Run ID: <guid>
Trigger: Manual
Trigger time: 2026-10-27T18:02:11Z
Processed: 2025-March

Bronze records: 296
Silver records: 291
Gold fact (staging) records: 291
```

(The numbers above are illustrative.)

## 7. Gmail filters

Mail is sent from your own address to your own address. Two filters keep it tidy. Gmail ignores square brackets, so match on the words.

| Filter | From | Subject | Action |
|---|---|---|---|
| Failures | your address | `FAILED Ecommerce_Returns_Pipeline` | Apply label `Fabric/Pipeline Failed`, star, always mark important, never send to spam |
| Successes | your address | `SUCCEEDED Ecommerce_Returns_Pipeline` | Skip the inbox, apply label `Fabric/Pipeline Succeeded`, never send to spam |

The labelled mail doubles as a run log (run IDs, counts, errors) that is easier to search than the Fabric Monitor time filter.

## 8. Security and limits

- Anyone who has the `/exec` URL can trigger an email from your account. Keep it in the Fabric connection, and **never commit it to the repo or paste it in an issue**. If it leaks, create a new deployment (new URL) and update the connection.
- The email contains the run ID, the month and the error text. Do not add personal data to the body.
- Free Gmail accounts can send about 100 emails a day through Apps Script, far more than this pipeline needs.
- Apps Script web apps answer POST requests with a redirect; the Fabric Web activity follows it.

## 9. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| No email arrives | Check spam. Open the Web activity output in Fabric Monitor. Check **Apps Script → Executions** for errors. |
| Web activity fails with 401/403 | The deployment's access is not **Anyone**, or the connection URL is wrong. Redeploy with access **Anyone**. |
| Web activity fails with 404 | The base URL is not the `/exec` URL, or the deployment was deleted. |
| Subject always says FAILED | The body does not start with a `Status:` line, or the script is still the old version. Deploy a new version. |
| Email body shows `@concat(...)` text | The body was pasted as plain text. Re-enter it through **Add dynamic content** so it is saved as an expression. |
| Pipeline fails with the email error instead of the real error | `Fail_Pipeline` is connected on **Succeeded** instead of **Completed**. Change the arrow. |
| No success email even though the run worked | Check that the arrow from `Stored_Procedure_Insert_Metadata` into `If_Check_Error` is **Completed** (blue), not Succeeded. Then check the `Send_Success_Email` output in Fabric Monitor and **Apps Script → Executions**. |
| Email is slow or the pipeline waits | The script is hanging. Fabric's minimum Web activity timeout is 10 minutes, so each attempt can wait that long. Keep retries at 1 and check **Apps Script → Executions**. |
