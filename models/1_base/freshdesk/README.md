# Freshdesk

Freshdesk data is loaded through Airbyte. The job loads data into the `ext_airbyte_freshdesk` schema. Currently the Freshdesk DBT models are not configured to be late-binding views. That means any change in column data type, or a column drop, will throw an error to the actor. This is done as a security measure as the models should not change (often).