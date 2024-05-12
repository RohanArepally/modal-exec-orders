import modal 

app = modal.App("crawl-and-index-executive-orders")
image = modal.Image.debian_slim().pip_install("requests", )

vol = modal.Volume.from_name("exec-order-data", create_if_missing=True)

BIDEN_SEED_URL = "https://www.federalregister.gov/api/v1/documents.json?conditions%5Bcorrection%5D=0&conditions%5Bpresident%5D=joe-biden&conditions%5Bpresidential_document_type%5D=executive_order&conditions%5Btype%5D%5B%5D=PRESDOCU&fields%5B%5D=citation&fields%5B%5D=document_number&fields%5B%5D=end_page&fields%5B%5D=html_url&fields%5B%5D=pdf_url&fields%5B%5D=type&fields%5B%5D=subtype&fields%5B%5D=publication_date&fields%5B%5D=signing_date&fields%5B%5D=start_page&fields%5B%5D=title&fields%5B%5D=disposition_notes&fields%5B%5D=executive_order_number&fields%5B%5D=not_received_for_publication&fields%5B%5D=full_text_xml_url&fields%5B%5D=body_html_url&fields%5B%5D=json_url&include_pre_1994_docs=true&maximum_per_page=10000&order=executive_order&per_page=10000"
TRUMP_SEED_URL = "https://www.federalregister.gov/api/v1/documents.json?conditions%5Bcorrection%5D=0&conditions%5Bpresident%5D=donald-trump&conditions%5Bpresidential_document_type%5D=executive_order&conditions%5Btype%5D%5B%5D=PRESDOCU&fields%5B%5D=citation&fields%5B%5D=document_number&fields%5B%5D=end_page&fields%5B%5D=html_url&fields%5B%5D=pdf_url&fields%5B%5D=type&fields%5B%5D=subtype&fields%5B%5D=publication_date&fields%5B%5D=signing_date&fields%5B%5D=start_page&fields%5B%5D=title&fields%5B%5D=disposition_notes&fields%5B%5D=executive_order_number&fields%5B%5D=not_received_for_publication&fields%5B%5D=full_text_xml_url&fields%5B%5D=body_html_url&fields%5B%5D=json_url&include_pre_1994_docs=true&maximum_per_page=10000&order=executive_order&per_page=10000"
OBAMA_SEED_URL = "https://www.federalregister.gov/api/v1/documents.json?conditions%5Bcorrection%5D=0&conditions%5Bpresident%5D=barack-obama&conditions%5Bpresidential_document_type%5D=executive_order&conditions%5Btype%5D%5B%5D=PRESDOCU&fields%5B%5D=citation&fields%5B%5D=document_number&fields%5B%5D=end_page&fields%5B%5D=html_url&fields%5B%5D=pdf_url&fields%5B%5D=type&fields%5B%5D=subtype&fields%5B%5D=publication_date&fields%5B%5D=signing_date&fields%5B%5D=start_page&fields%5B%5D=title&fields%5B%5D=disposition_notes&fields%5B%5D=executive_order_number&fields%5B%5D=not_received_for_publication&fields%5B%5D=full_text_xml_url&fields%5B%5D=body_html_url&fields%5B%5D=json_url&include_pre_1994_docs=true&maximum_per_page=10000&order=executive_order&per_page=10000"

SEED_URL_BY_PRES = {
    "Obama": OBAMA_SEED_URL,
    "Trump": TRUMP_SEED_URL,
    "Biden": BIDEN_SEED_URL,
}

@app.function(image=image, volumes={"/data": vol})
async def crawl_and_store_orders(president: str, json_url: str):
    import requests 
    response = requests.get(json_url)
    data = response.json()

    for result in data["results"]:
        document_url = result["json_url"]
        response = requests.get(document_url)
        document_data = response.json()
        raw_text_url = document_data["raw_text_url"]
        raw_text = requests.get(raw_text_url)

        with open(f"/data/{president}_{document_data['presidential_document_number']}.txt", "w") as f:
            f.write(raw_text.text)
        vol.commit()  # Needed to make sure all changes are persisted

        print(f"Persisted {president} : {document_data['presidential_document_number']}")

@app.function(image=image, volumes={"/data": vol})
async def main():
    for k,v in SEED_URL_BY_PRES.items():
        await crawl_and_store_orders.local(k, v)
    