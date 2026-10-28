/**
 * Fabric pipeline email notifier (Google Apps Script web app).
 *
 * The Fabric Web activity POSTs a plain-text body. The first lines look like:
 *   Status: SUCCEEDED        (or FAILED)
 *   Pipeline: Ecommerce_Returns_Pipeline
 *   Run ID: ...
 * The script reads the Status and Pipeline lines for the subject and sends the whole
 * body as the email. If the Status line is missing, the mail is treated as FAILED.
 *
 * Deploy: Deploy > New deployment > Web app
 *   Execute as: Me        Who has access: Anyone
 * Copy the /exec URL into the Fabric Web v2 connection (never commit that URL).
 * To change this script later, use Deploy > Manage deployments > Edit > New version
 * so the URL stays the same.
 */

// Change this to the address that should receive the alerts.
const TO = 'your.email@gmail.com';

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
