#!/bin/bash
echo "$SERVICE_ACCOUNT_JSON" > service_account.json
python3 fetch_oi_futures.py
