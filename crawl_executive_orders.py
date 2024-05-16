import modal 
import pathlib
from pydantic import BaseModel
from datetime import datetime


app = modal.App("crawl-and-index-executive-orders")
image = modal.Image.debian_slim().pip_install("requests", "anthropic", "instructor")

vol = modal.Volume.from_name("exec-order-data", create_if_missing=True)

data_by_pres_and_id = modal.Dict.from_name("president-data-dict", create_if_missing=True)


BIDEN_SEED_URL = "https://www.federalregister.gov/api/v1/documents.json?conditions%5Bcorrection%5D=0&conditions%5Bpresident%5D=joe-biden&conditions%5Bpresidential_document_type%5D=executive_order&conditions%5Btype%5D%5B%5D=PRESDOCU&fields%5B%5D=citation&fields%5B%5D=document_number&fields%5B%5D=end_page&fields%5B%5D=html_url&fields%5B%5D=pdf_url&fields%5B%5D=type&fields%5B%5D=subtype&fields%5B%5D=publication_date&fields%5B%5D=signing_date&fields%5B%5D=start_page&fields%5B%5D=title&fields%5B%5D=disposition_notes&fields%5B%5D=executive_order_number&fields%5B%5D=not_received_for_publication&fields%5B%5D=full_text_xml_url&fields%5B%5D=body_html_url&fields%5B%5D=json_url&include_pre_1994_docs=true&maximum_per_page=10000&order=executive_order&per_page=10000"
TRUMP_SEED_URL = "https://www.federalregister.gov/api/v1/documents.json?conditions%5Bcorrection%5D=0&conditions%5Bpresident%5D=donald-trump&conditions%5Bpresidential_document_type%5D=executive_order&conditions%5Btype%5D%5B%5D=PRESDOCU&fields%5B%5D=citation&fields%5B%5D=document_number&fields%5B%5D=end_page&fields%5B%5D=html_url&fields%5B%5D=pdf_url&fields%5B%5D=type&fields%5B%5D=subtype&fields%5B%5D=publication_date&fields%5B%5D=signing_date&fields%5B%5D=start_page&fields%5B%5D=title&fields%5B%5D=disposition_notes&fields%5B%5D=executive_order_number&fields%5B%5D=not_received_for_publication&fields%5B%5D=full_text_xml_url&fields%5B%5D=body_html_url&fields%5B%5D=json_url&include_pre_1994_docs=true&maximum_per_page=10000&order=executive_order&per_page=10000"
OBAMA_SEED_URL = "https://www.federalregister.gov/api/v1/documents.json?conditions%5Bcorrection%5D=0&conditions%5Bpresident%5D=barack-obama&conditions%5Bpresidential_document_type%5D=executive_order&conditions%5Btype%5D%5B%5D=PRESDOCU&fields%5B%5D=citation&fields%5B%5D=document_number&fields%5B%5D=end_page&fields%5B%5D=html_url&fields%5B%5D=pdf_url&fields%5B%5D=type&fields%5B%5D=subtype&fields%5B%5D=publication_date&fields%5B%5D=signing_date&fields%5B%5D=start_page&fields%5B%5D=title&fields%5B%5D=disposition_notes&fields%5B%5D=executive_order_number&fields%5B%5D=not_received_for_publication&fields%5B%5D=full_text_xml_url&fields%5B%5D=body_html_url&fields%5B%5D=json_url&include_pre_1994_docs=true&maximum_per_page=10000&order=executive_order&per_page=10000"

SEED_URL_BY_PRES = {
    "Obama": OBAMA_SEED_URL,
    "Trump": TRUMP_SEED_URL,
    "Biden": BIDEN_SEED_URL,
}

@app.function(image=image, volumes={"/data": vol}, allow_concurrent_inputs=1)
def crawl_and_store_orders(president: str, json_url: str, page_no: int = 1):
    import requests 
    response = requests.get(json_url)
    data = response.json()

    if "next_page_url" in data: 
        next_page_url = data["next_page_url"]
        crawl_and_store_orders.spawn(president, next_page_url, page_no+1)

    for result in data["results"]:
        document_url = result["json_url"]
        response = requests.get(document_url)
        document_data = response.json()
        raw_text_url = document_data["raw_text_url"]
        raw_text = requests.get(raw_text_url)

        p = pathlib.Path(f"/data/{president}_{document_data['presidential_document_number']}.txt")
        if p.exists():
            continue

        with open(f"/data/{president}_{document_data['presidential_document_number']}.txt", "w") as f:
            f.write(raw_text.text)
        vol.commit()  # Needed to make sure all changes are persisted

        print(f"Persisted {president} : {document_data['presidential_document_number']}")
 

@app.function(image=image, volumes={"/data": vol}, allow_concurrent_inputs=1)
def main():
    list(crawl_and_store_orders.starmap(SEED_URL_BY_PRES.items()))



class CategorizationResult(BaseModel):
    subject_category: str
    is_symbolic_or_ceremonial: bool
    date: datetime


@app.function(
    image=image, 
    volumes={"/data": vol}, 
    secrets=[modal.Secret.from_name("my-anthropic-secret")], 
)
def categorize(president: str, doc_number: int, body: str):
    import instructor
    from anthropic import Anthropic 


    if f"{president}_{doc_number}" not in data_by_pres_and_id:
        subject_categories = [
            "Immigration",
            "Environment and Climate Change",
            "Healthcare",
            "Education",
            "Foreign Policy and National Security",
            "Economy and Trade",
            "Civil Rights and Social Justice",
            "Government Operations and Accountability",
            "Infrastructure and Transportation",
            "Science and Technology"
        ]

        client = instructor.from_anthropic(Anthropic()) 

        messages = [
            {"role": "user", "content": f"Categorize the following executive order based on its primary subject matter (must be in Valid Subject Categories) and indicate if it's a symbolic or ceremonial action. Also, provide the date of the executive order. \n\n{body}"},
            {"role": "assistant", "content": f"Valid Subject Categories: {', '.join(subject_categories)}"},
        ]

        response = client.messages.create(
            model="claude-3-sonnet-20240229",  
            max_tokens=150,  
            messages=messages,
            response_model=CategorizationResult,
        )

        data_by_pres_and_id[f"{president}_{doc_number}"] = response
        print(repr(data_by_pres_and_id[f"{president}_{doc_number}"]))
    else:
        # Skip categorization if the document has already been processed
        pass


@app.function(schedule=modal.Period(minutes=2), image=image, volumes={"/data": vol}, secrets=[modal.Secret.from_name("my-anthropic-secret")])
def categorize_all():
    for f in vol.listdir("/"):
        p = f.path
        parts = p.split("_")
        president, doc_number = parts[0], int(parts[1][:-4])
        
        if f"{president}_{doc_number}" not in data_by_pres_and_id:
            with open(f"/data/{president}_{doc_number}.txt", "r") as f:
                body = f.read()

            categorize.local(president, doc_number, body)