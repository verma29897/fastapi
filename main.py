import os
import typing as ty
import aiofiles
import aiohttp
import aiohttp.client
import uvicorn
import asyncio
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
import random

app = FastAPI()

# Define the Pydantic model for request body
class MessageRequest(BaseModel):
    phone_number_id: str
    template_name: str
    language: str
    media_type: str
    media_id: ty.Optional[str]
    contact_list: ty.List[str]
AUTH_TOKEN_list=["EAAOPkGzfCvsBO7NU41plquDk840FLMk08lvHl5rl2vmWaeQosYKLokzLxCWjt9emieWpSXdVA2Ah1NfPUWdDbPn8i10rzTh7GZAa778vpOUCsHPGWcZBi8a3OYMAM5HZBNeT0PL58AnAFEtYw25AH6ROBsw3RnJgUu5bkwJX6lUiymPBNIYqhz6AXTUOP8K8AZDZD",
"EAAOPkGzfCvsBOwHdoQlgCA8YWPMEH9Vg8NJlUyeqSWmD5Qf8XPmbPHNLuB5J3DYy5KGlbBpixT1X5RykHOwSQnMl07ZAgAGxt7431cqZBRPvsqEkFSXgbtQBj25SibKrhXP6IzMWhtsQHOlNsaDuJBdPuTZA6ZCeyLkdz0UdbZAoCezyFmx6jAZCAiG5MhY2PO90KUXsV65QLopt0FlGRtKIfFiKNvbPA2fQ5AHOUZD",
"EAAOPkGzfCvsBO16W56egEytiLeCZAMERkZCaJPa3d98WZAX3Vj04tKwjxU1O84HSUzpSMFrBKn3vQ6sP7S2jTfkoaBFHGonTZB1aLnSwxIRdmVoZBijAIF0nI3Y2FilqtvWAkFTC35ob4ySyac4NeN2ZCBUxCN6KnAY2bua8jmZCNGAGpERX6n3BrzNtx4bmIMyDAZDZD",
"EAAOPkGzfCvsBOZCloHZBSZBjZCWcu70l2ZBjzV3ng0VbnyArxwBipPZAjHr1XY0NnsynAdcvLc9PIwYDPjVZAXUCTZColmCgj65gZAqmNL9lFo98D6YZCbllGZAyuTYS5KM2xrapGgIxjzXPh1ZB985SYDfenf59YQStfrSOBClieJHlKmcZAkZCBiTz5OOgvoYZC6AnRypBAZDZD"]
async def send_message(session: aiohttp.ClientSession,phone_number_id: str,template_name: str,language: str,media_type: str,media_id: ty.Optional[str],contact: str) -> None:
    AUTH_TOKEN=random.choice(AUTH_TOKEN_list)
    url = f"https://graph.facebook.com/v20.0/{phone_number_id}/messages"
    headers = {
        "Authorization": f"Bearer {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    header_component = {
        "type": "header",
        "parameters": []
    }

    if media_id and media_type in ["IMAGE", "DOCUMENT", "VIDEO", "AUDIO"]:
        header_component["parameters"].append({
            "type": media_type.lower(),
            media_type.lower(): {"id": media_id}
        })

    payload = {
        "messaging_product": "whatsapp",
        "to": contact,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": language},
            "components": [
                header_component,
                {"type": "body", "parameters": []}
            ]
        }
    }

    try:
        async with session.post(url, json=payload, headers=headers) as response:
            if response.status != 200:
                #error_message = await response.text()
                return
                #raise HTTPException(status_code=response.status, detail=f"Failed to send message: {error_message}")
    except aiohttp.ClientError as e:
        return
        #raise HTTPException(status_code=500, detail=f"Error sending message: {e}")
async def send_messages(phone_number_id: str,template_name: str,language: str,media_type: str,media_id: ty.Optional[str],contact_list: ty.List[str]) -> None:
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1000)) as session:
        for batch in chunks(contact_list, 1000):
            tasks = [send_message(session, phone_number_id, template_name, language, media_type, media_id, contact) for contact in batch]
            await asyncio.gather(*tasks)

def chunks(lst: ty.List[str], size: int) -> ty.Generator[ty.List[str], None, None]:
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

@app.post("/send_sms/")
async def send_messages_api(request: MessageRequest):
    try:
        await send_messages(
            phone_number_id=request.phone_number_id,
            template_name=request.template_name,
            language=request.language,
            media_type=request.media_type,
            media_id=request.media_id,
            contact_list=request.contact_list
        )
        return {'message': 'Messages sent successfully'}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {e}")

@app.get("/")
def root():
    return {"message": "Successful"}

if __name__ == '__main__':
    uvicorn.run(app, host="127.0.0.1",port=8000)
