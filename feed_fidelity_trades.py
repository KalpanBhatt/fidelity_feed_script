import pandas
import json
import datetime
import re
import uuid
import requests

df = pandas.read_csv('Fidelity_Accounts_History.csv')
#print(df)


records = (pandas.DataFrame.to_json(df,orient = 'records'))
records = json.loads(records)

for trade in records:
	if not (
		"JOURNALED JNL" in trade["Action"] or 
		"JOURNALED SPP PURCHASE CREDIT" in trade["Action"] or
		"REINVESTMENT FIDELITY GOVERNMENT MONEY MARKET" in trade["Action"] ):

		symbol = trade["Symbol"].strip()
		option_trade_flag = True if len(symbol.split('-'))>1 else False

		account_name = ""
		if "Personal Trading" in trade["Account"]:
			account_name = "Fidelity Personal Trading"
		elif "ESPP Executed" in trade["Account"]:
			account_name = "Fidelity ESPP Comcast"

		transaction_type = "Unknown"
		if "YOU SOLD" in trade["Action"]:
			transaction_type = "Sell"
		elif ("YOU BOUGHT" in trade["Action"]) or ("REINVESTMENT" in trade["Action"]):
			transaction_type = "Buy"
		elif "DISTRIBUTION" in trade["Action"]:
			transaction_type = "SPLIT"
		elif "REVERSE SPLIT" in trade["Action"]:
			transaction_type = "R-SPLIT"
		elif ("DIVIDEND RECEIVED" in trade["Action"]) or ("RETURN OF CAPITAL" in trade["Action"]):
			transaction_type = "Dividend"
		elif "DEBIT CARD PURCHASE" in trade["Action"]:
			transaction_type = "Bill Pay"
		elif "LONG-TERM CAP GAIN" in trade["Action"]:
			transaction_type = "Long-term Cap Gain"
		elif "SHORT-TERM CAP GAIN" in trade["Action"]:
			transaction_type = "Short-term Cap Gain"
		elif "Electronic Funds Transfer Received" in trade["Action"]:
			transaction_type = "Funds Received"
		elif "Electronic Funds Transfer Paid" in trade["Action"]:
			transaction_type = "Funds Withdrawn"
		elif ("TRANSFERRED TO" in trade["Action"]) or ("TRANSFERRED FROM" in trade["Action"]):
			transaction_type = "Internal Transfer"
		elif "JOURNALED STOCK PLAN DIVIDEND" in trade["Action"]:
			transaction_type = "Dividend"
			symbol = "CMCSA"

		op_trade_ef = None
		if option_trade_flag and "EXPIRED" in trade["Action"]:
			op_trade_ef = "Expired"
			transaction_type = "Op-Expired"
		elif option_trade_flag and "OPENING TRANSACTION" in trade["Action"]:
			op_trade_ef = "open"
		elif option_trade_flag and "CLOSING TRANSACTION" in trade["Action"]:
			op_trade_ef = "close"
		elif option_trade_flag and "ASSIGNED" in trade["Action"]:
			op_trade_ef = "Assigned"
			transaction_type = "Op-Assigned"


		tags = []
		if transaction_type == "Bill Pay" and "COMCAST" in trade["Action"]:
			tags = ["Bill", "Comcast"]
		elif transaction_type == "Bill Pay" and "ELIZABETHTOWN" in trade["Action"]:
			tags = ["Bill", "Elizabethtown Gas"]


		elk_rq_payload = {
			"transaction_id": str(uuid.uuid4()),
	    	"trade_date": str(datetime.datetime.strptime(trade["Run Date"].strip(),'%m/%d/%Y')).split(' ')[0],
	    	"symbol": symbol if not(option_trade_flag) else re.split('(\d+)', symbol)[0][1:],
	    	"account_name": account_name,
	    	"account_number": trade["Account"].split('X')[1],
	    	"transaction_type": transaction_type,
	    	"security_name": trade["Security Description"].strip(),
	    	"security_type": trade["Security Type"].strip(),
	    	"quantity": (trade["Quantity"]),
		    "multiplier": 100 if option_trade_flag else 1,
		    "unit_price": trade["Price ($)"],
		    "fees": trade["Fees ($)"],
		    "commission": trade["Commission ($)"],
		    "net_amount": trade["Amount ($)"],
		    "accrued_interest": trade["Accrued Interest ($)"],
		    "verified": False,
		    "created_at": str(datetime.datetime.now()),
		    "updated_at": str(datetime.datetime.now()),
		    "source": "python-script",
	    	"options_symbol": symbol if option_trade_flag else None,
	    	"options_expiry": re.split('(\d+)', symbol)[1] if option_trade_flag else None,
	    	"options_strategy": None,
	    	"options_trade_effect": op_trade_ef,
	    	"notes": None,
	    	"tags": tags if len(tags)>0 else None,
	    	"raw": str(trade)

		}
		#print(elk_rq_payload)
		elk_rs_payload = requests.post("http://localhost:9200/trade-records/_doc/",json=elk_rq_payload)
		if (elk_rs_payload.status_code != 201):
				print(elk_rs_payload.json())
		

print("Done")	