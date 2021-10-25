# ORDERS

## Refactor Context

In mid-2021 we did a significant refactor on our legacy tables cube deals and fact deals, in our legacy reporting schema.
The purpose of this was to significantly tidy up the code as it was taking too long to run and it was highly difficult to debug.
We took the old cube deals code and separated it into different models in DBT that would make the logic more structured and understanble.
The table was reduced from 300 columns to 150, ~2300 lines of code to ~1300 split across different models and time to load down from 15 mins to 5 mins.


## New Structure

![Screenshot 2021-10-25 at 10 10 41](https://user-images.githubusercontent.com/61149777/138658725-816018f6-42cb-40fc-b2a9-f9ee0ea809d1.png)
