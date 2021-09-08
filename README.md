# ss-update
This sample code copies data from smartsheet and write to Appsheet using Smartsheet API.

What this code do are as follows:

1 Customer inputs data through Smartsheet Form and a corresponding Sheet captures data.

2 Then using Smarthsset API, correspoinding data is copied to Appsheet.

  1) Use Smartsheet API to get the inputed data.
  2) Get correspoing data from a GCP Mysql database
  3) Write back the data to another service Appsheet

I chose Smartsheet Form instead of Appsheet Form as it displays many rows of choices more user-friendly.
