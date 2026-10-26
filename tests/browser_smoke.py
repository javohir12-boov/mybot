"""Opt-in mobile/browser smoke check using an installed Chrome, no npm packages.

Run: .venv/Scripts/python.exe tests/browser_smoke.py
Uses the API suite's isolated DB and fake AI; never contacts a real bot.
"""

import asyncio
import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aiohttp import ClientSession
from test_webapp import ApiTests, QUESTION, signed
from services.interactive_api import SERVICE
from services.support_service import SupportService
from services import database as db
from services.webapp import STATE


async def run():
    chrome = Path(os.environ.get("PROGRAMFILES", "C:/Program Files")) / "Google/Chrome/Application/chrome.exe"
    if not chrome.exists():
        raise RuntimeError("Install Chrome or update the executable path for your platform.")
    root = Path(__file__).resolve().parent.parent
    output = root / "downloads" / "webapp-qa"
    output.mkdir(parents=True, exist_ok=True)
    fixture = ApiTests("test_static_app_and_api_authentication")
    await fixture.asyncSetUp()
    bot_quiz_id = await db.create_quiz(creator_id=101, title="Botda avval yaratilgan test")
    await db.create_questions_bulk(bot_quiz_id, [QUESTION])
    browser = None
    exceptions = []
    try:
        with tempfile.TemporaryDirectory(dir=output) as profile:
            browser_log = (output / "chrome.log").open("wb")
            browser = subprocess.Popen([
                str(chrome), "--headless=new", "--no-sandbox", "--disable-gpu", "--disable-software-rasterizer", "--no-first-run", "--no-default-browser-check",
                "--remote-debugging-port=0", f"--user-data-dir={profile}", "about:blank",
            ], stdout=subprocess.DEVNULL, stderr=browser_log, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            port = None
            try:
                devtools = Path(profile) / "DevToolsActivePort"
                for _ in range(150):
                    try:
                        lines = devtools.read_text().splitlines()
                        if lines and lines[0].isdigit():
                            port = lines[0]
                            break
                    except OSError:
                        pass  # Chrome may still hold the file while writing it on Windows.
                    await asyncio.sleep(0.1)
                if not port:
                    raise AssertionError(f"Chrome did not open DevTools; see {output / 'chrome.log'}")
                async with ClientSession() as session:
                    async with session.get(f"http://127.0.0.1:{port}/json/list") as response:
                        pages = await response.json()
                    page = next(p for p in pages if p["type"] == "page")
                    async with session.ws_connect(page["webSocketDebuggerUrl"]) as ws:
                        seq = 0

                        async def cdp(method, params=None):
                            nonlocal seq
                            seq += 1
                            current = seq
                            await ws.send_json({"id": current, "method": method, "params": params or {}})
                            async with asyncio.timeout(20):
                                async for message in ws:
                                    data = json.loads(message.data)
                                    if data.get("method") == "Runtime.exceptionThrown":
                                        exceptions.append(data["params"]["exceptionDetails"])
                                    if data.get("id") == current:
                                        if "error" in data:
                                            raise AssertionError(data["error"])
                                        return data.get("result", {})
                            raise AssertionError(f"Chrome disconnected during {method}; see {output / 'chrome.log'}")

                        async def evaluate(source):
                            result = await cdp("Runtime.evaluate", {"expression": source, "returnByValue": True, "awaitPromise": True})
                            if result.get("exceptionDetails"):
                                raise AssertionError(result["exceptionDetails"])
                            return result.get("result", {}).get("value")

                        async def wait_for(source):
                            for _ in range(150):
                                if await evaluate(source):
                                    return
                                await asyncio.sleep(0.1)
                            raise AssertionError(f"Browser condition timed out: {source}")

                        async def screenshot(name, width=390, height=844):
                            await cdp("Emulation.setDeviceMetricsOverride", {"width": width, "height": height, "deviceScaleFactor": 1, "mobile": True})
                            await evaluate("window.scrollTo(0,0)")
                            await asyncio.sleep(0.2)
                            assert await evaluate("document.documentElement.scrollWidth <= window.innerWidth"), f"Horizontal overflow at {width}px"
                            shot = await cdp("Page.captureScreenshot", {"format": "png", "captureBeyondViewport": False})
                            (output / name).write_bytes(base64.b64decode(shot["data"]))

                        await cdp("Page.enable")
                        await cdp("Runtime.enable")
                        await cdp("Network.enable")
                        await cdp("Network.setBlockedURLs", {"urls": ["*telegram.org/js/*", "*fonts.googleapis.com/*", "*fonts.gstatic.com/*"]})
                        fake = "window.Telegram={WebApp:{initData:" + json.dumps(signed()) + ",colorScheme:'light',ready(){},expand(){},setHeaderColor(){},setBackgroundColor(){},isVersionAtLeast(){return true},setBottomBarColor(){},onEvent(){},openTelegramLink(url){window.lastTelegramLink=url},BackButton:{onClick(){},show(){},hide(){}}}};"
                        await cdp("Page.addScriptToEvaluateOnNewDocument", {"source": fake})
                        await cdp("Emulation.setDeviceMetricsOverride", {"width": 390, "height": 844, "deviceScaleFactor": 1, "mobile": True})
                        url = str(fixture.client.make_url("/app"))
                        with patch("services.webapp.AIService", return_value=SimpleNamespace(generate_quiz_from_topic=AsyncMock(return_value=[QUESTION]))):
                            await cdp("Page.navigate", {"url": url})
                            await wait_for("document.querySelector('#profile-shortcut')?.textContent === 'O'")
                            assert await evaluate("document.querySelectorAll('.nav-item').length === 5")
                            await screenshot("file-mobile.png")
                            await screenshot("file-small.png", 320, 740)
                            await evaluate("location.hash='file'")
                            await evaluate("document.querySelector('[data-go=topic]').click();document.querySelector('.brand').click()")
                            await wait_for("!!document.querySelector('#file-form')")
                            await evaluate("document.querySelector('[data-go=topic]').click()")
                            await wait_for("!!document.querySelector('#topic-form')")
                            await evaluate("document.querySelector('[name=topic]').value='Mathematics';document.querySelector('#topic-form').requestSubmit()")
                            await wait_for("!!document.querySelector('[data-start]')")
                            await evaluate("document.querySelector('[data-start]').click()")
                            assert "?start=quiz_" in await evaluate("window.lastTelegramLink")
                            await screenshot("quiz-mobile.png")
                            await evaluate("document.querySelector('[data-go=manual]').click()")
                            await wait_for("!!document.querySelector('#manual-form')")
                            await evaluate("document.querySelector('[name=title]').value='Browser quiz';document.querySelector('[name=question]').value='2 + 2 = ?';document.querySelectorAll('[name=answer]').forEach((e,i)=>e.value=String(i+1));document.querySelectorAll('[name=correct-0]')[3].checked=true;document.querySelector('#manual-form').dispatchEvent(new Event('input',{bubbles:true}))")
                            await screenshot("manual-mobile.png")
                            await evaluate("document.querySelector('#manual-form').requestSubmit()")
                            await wait_for("document.querySelector('h1')?.textContent==='Browser quiz'")
                            assert await evaluate("localStorage.getItem('quiz-draft:101')===null"), "Saved manual quiz must clear its draft"
                            await evaluate("document.querySelector('[data-go=profile]').click()")
                            await wait_for("!!document.querySelector('#ui-language')")
                            await screenshot("profile-mobile.png")
                            await evaluate("document.querySelector('[data-go=profile-edit]').click()")
                            await wait_for("!!document.querySelector('#profile-form')")
                            await evaluate("document.querySelector('[name=display_name]').value='Ali Valiyev';document.querySelector('[name=profile_username]').value='ali_work';document.querySelector('#profile-form').dispatchEvent(new Event('input',{bubbles:true}))")
                            await evaluate("(async()=>{const canvas=document.createElement('canvas');canvas.width=100;canvas.height=300;const ctx=canvas.getContext('2d');for(const [i,color] of ['red','green','blue'].entries()){ctx.fillStyle=color;ctx.fillRect(0,i*100,100,100)}const blob=await new Promise(resolve=>canvas.toBlob(resolve,'image/png'));const transfer=new DataTransfer();transfer.items.add(new File([blob],'avatar.png',{type:'image/png'}));const input=document.querySelector('#avatar-input');input.files=transfer.files;input.dispatchEvent(new Event('change',{bubbles:true}));})()")
                            await wait_for("!!document.querySelector('.avatar-cropper canvas')")
                            assert await evaluate("document.querySelector('.avatar-cropper canvas').getContext('2d').getImageData(384,384,1,1).data[1]===128"), "Portrait should initially show its center"
                            await evaluate("document.querySelector('[data-avatar-fit]').click()")
                            assert await evaluate("(()=>{const c=document.querySelector('.avatar-cropper canvas').getContext('2d');return c.getImageData(384,100,1,1).data[0]===255&&c.getImageData(384,668,1,1).data[2]===255})()"), "Fit must preserve both ends of a long photo"
                            await screenshot("avatar-fit-mobile.png", 390, 844)
                            await evaluate("document.querySelector('[data-avatar-reset]').click()")
                            box = await evaluate("(()=>{const r=document.querySelector('.avatar-crop-stage').getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2,size:r.width}})()")
                            await evaluate("window.cropPointerEvents=[];for(const n of ['pointerdown','pointermove','pointerup'])document.querySelector('.avatar-crop-stage').addEventListener(n,e=>window.cropPointerEvents.push([e.type,e.clientX,e.clientY,e.pointerType]))")
                            await cdp("Input.dispatchMouseEvent", {"type": "mousePressed", "x": box["x"], "y": box["y"], "button": "left", "clickCount": 1})
                            await cdp("Input.dispatchMouseEvent", {"type": "mouseMoved", "x": box["x"], "y": box["y"] + box["size"], "button": "left", "buttons": 1})
                            await asyncio.sleep(0.1)
                            await cdp("Input.dispatchMouseEvent", {"type": "mouseReleased", "x": box["x"], "y": box["y"] + box["size"], "button": "left"})
                            assert await evaluate("document.querySelector('.avatar-cropper canvas').getContext('2d').getImageData(384,384,1,1).data[0]===255"), {"message": "Dragging should select the top of the portrait", "box": box, "events": await evaluate("window.cropPointerEvents"), "exceptions": exceptions}
                            await evaluate("document.querySelector('[data-avatar-reset]').click()")
                            await cdp("Emulation.setTouchEmulationEnabled", {"enabled": True})
                            await cdp("Input.dispatchTouchEvent", {"type": "touchStart", "touchPoints": [{"x": box["x"] - 40, "y": box["y"]}, {"x": box["x"] + 40, "y": box["y"]}]})
                            await cdp("Input.dispatchTouchEvent", {"type": "touchMove", "touchPoints": [{"x": box["x"] - 60, "y": box["y"]}, {"x": box["x"] + 60, "y": box["y"]}]})
                            await cdp("Input.dispatchTouchEvent", {"type": "touchEnd", "touchPoints": []})
                            assert await evaluate("Number(document.querySelector('.avatar-zoom input').value)>1.3"), "Pinch should zoom the crop"
                            await evaluate("document.querySelector('[data-avatar-reset]').click();const stage=document.querySelector('.avatar-crop-stage');for(let i=0;i<40;i++)stage.dispatchEvent(new KeyboardEvent('keydown',{key:'ArrowDown',bubbles:true}));const slider=document.querySelector('.avatar-zoom input');slider.value='1.5';slider.dispatchEvent(new Event('input',{bubbles:true}))")
                            assert await evaluate("document.querySelector('.avatar-cropper canvas').getContext('2d').getImageData(384,384,1,1).data[0]===255"), "Moving and zooming should select the top of the portrait"
                            await screenshot("profile-edit-mobile.png", 390, 844)
                            await evaluate("document.querySelector('#profile-form').requestSubmit()")
                            await wait_for("document.querySelector('.profile-hero h2')?.textContent==='Ali Valiyev' && !!document.querySelector('.profile-hero img')")
                            assert "@ali_work" in await evaluate("document.querySelector('.profile-hero').textContent")
                            assert await evaluate("(async()=>{const img=document.querySelector('.profile-hero img');await img.decode();const c=document.createElement('canvas');c.width=c.height=img.naturalWidth;c.getContext('2d').drawImage(img,0,0);return img.naturalWidth===img.naturalHeight&&c.getContext('2d').getImageData(192,192,1,1).data[0]===255})()"), "Saved avatar must keep the selected area"
                            await cdp("Page.navigate", {"url": url})
                            await wait_for("!!document.querySelector('#profile-shortcut img')")
                            await evaluate("document.querySelector('[data-go=profile]').click()")
                            await wait_for("document.querySelector('.profile-hero h2')?.textContent==='Ali Valiyev'")
                            await screenshot("profile-saved-mobile.png", 390, 844)
                            for mime in ["image/jpeg", "image/webp"]:
                                await evaluate("document.querySelector('[data-go=profile-edit]').click()")
                                await wait_for("!!document.querySelector('#profile-form')")
                                await evaluate("(async()=>{const canvas=document.createElement('canvas');canvas.width=20;canvas.height=20;const blob=await new Promise(resolve=>canvas.toBlob(resolve," + json.dumps(mime) + "));const transfer=new DataTransfer();transfer.items.add(new File([blob],'photo',{type:" + json.dumps(mime) + "}));const input=document.querySelector('#avatar-input');input.files=transfer.files;input.dispatchEvent(new Event('change',{bubbles:true}));})()")
                                await wait_for("!!document.querySelector('.avatar-cropper canvas')")
                                await evaluate("document.querySelector('#profile-form').requestSubmit()")
                                await wait_for("document.querySelector('.profile-hero h2')?.textContent==='Ali Valiyev' && !!document.querySelector('.profile-hero img')")
                            await evaluate("const lang=document.querySelector('#ui-language');lang.value='en';lang.dispatchEvent(new Event('change',{bubbles:true}))")
                            await wait_for("document.documentElement.lang==='en'")
                            assert await evaluate("document.querySelector('h1').textContent==='My profile'")
                            await evaluate("document.querySelector('[data-go=premium]').click()")
                            await wait_for("!!document.querySelector('[data-premium]')")
                            assert await evaluate("document.querySelectorAll('.plan').length===3")
                            await evaluate("document.querySelector('[data-premium]').click()")
                            assert await evaluate("window.lastTelegramLink.endsWith('start=webpremium')")
                            await evaluate("document.querySelector('[data-go=profile]').click()")
                            await wait_for("!!document.querySelector('[data-go=support]')")
                            await evaluate("document.querySelector('[data-go=quizzes]').click()")
                            await wait_for("!!document.querySelector('[data-quiz=\"" + str(bot_quiz_id) + "\"]')")
                            assert "Botda avval yaratilgan test" in await evaluate("document.querySelector('#main').textContent")
                            await screenshot("quizzes-mobile.png", 390, 844)
                            await evaluate("document.querySelector('[data-go=profile]').click()")
                            await evaluate("document.querySelector('[data-go=support]').click()")
                            await wait_for("!!document.querySelector('#support-form')")
                            fixture.bot.send_message = AsyncMock()
                            with patch("services.support_service.ADMIN_IDS", [999]), patch("services.webapp.support_service", SupportService()):
                                await evaluate("document.querySelector('[name=support_message]').value='Browser support test';document.querySelector('#support-form').requestSubmit()")
                                await wait_for("!!document.querySelector('.result-count[role=status]')")
                                assert "Browser support test" in await evaluate("document.querySelector('#support-messages').textContent")
                                fixture.bot.send_message.assert_awaited_once()
                                assert "Browser support test" in fixture.bot.send_message.await_args.args[1]
                            await screenshot("support-mobile.png", 390, 844)
                            await evaluate("document.querySelector('[data-go=profile]').click()")
                            await evaluate("document.querySelector('[data-go=bonuses]').click()")
                            await wait_for("!!document.querySelector('[data-share]')")
                            assert "start=ref_101" in await evaluate("document.querySelector('.ref-link').textContent")
                            await evaluate("document.querySelector('[data-go=profile]').click()")
                            await wait_for("!!document.querySelector('#ui-language')")
                            await evaluate("document.querySelector('#ui-language').value='uz';document.querySelector('#ui-language').dispatchEvent(new Event('change',{bubbles:true}))")
                            await wait_for("document.documentElement.lang==='uz'")
                            await evaluate("document.querySelector('[data-go=interactive]').click()")
                            await wait_for("document.querySelectorAll('[data-live-start]').length===5")
                            await screenshot("interactive-hub-mobile.png", 390, 844)
                            await screenshot("interactive-hub-small.png", 320, 740)
                            await evaluate("document.querySelector('[data-live-category=love]').click()")
                            await wait_for("document.querySelector('.live-category.selected')?.dataset.liveCategory==='love'")
                            await evaluate("document.querySelector('.live-add').click()")
                            await wait_for("!!document.querySelector('#interactive-create-form')")
                            assert await evaluate("document.querySelector('[name=live_kind]').value==='affinity'")
                            await evaluate("document.querySelector('[name=live_title]').value='Friendship check';document.querySelector('[name=live_question]').value='How much do you enjoy spending time together?';document.querySelectorAll('[name=live_option]').forEach((e,i)=>e.value=['Not much','A little','A lot','Very much'][i]);document.querySelectorAll('[name=live_weight]').forEach((e,i)=>e.value=[0,25,75,100][i]);document.querySelector('[name=live_period]').value='10';document.querySelector('#interactive-create-form').dispatchEvent(new Event('input',{bubbles:true}))")
                            await screenshot("interactive-create-mobile.png", 390, 844)
                            await evaluate("document.querySelector('#interactive-create-form').requestSubmit()")
                            await wait_for("document.querySelector('.live-quiz-card h3')?.textContent==='Friendship check'")
                            await evaluate("document.querySelector('[data-live-start]').click()")
                            await wait_for("document.querySelectorAll('[data-live-answer]').length===4")
                            room_id = await evaluate("new URLSearchParams(location.search).get('room')")
                            join = await fixture.client.post(f"/api/interactive/rooms/{room_id}/join", headers=fixture.headers(202))
                            assert join.status == 200
                            await evaluate("document.querySelector('[data-live-leave]').click()")
                            await wait_for("!!document.querySelector('.live-categories')")
                            answer = await fixture.client.post(f"/api/interactive/rooms/{room_id}/answers", headers=fixture.headers(202), json={"question_index": 0, "option_index": 2})
                            assert answer.status == 200
                            buddy = await fixture.client.get(f"/api/interactive/rooms/{room_id}", headers=fixture.headers(202))
                            assert (await buddy.json())["status"] == "active", "Host leaving must not stop the room"
                            await evaluate("document.querySelector('[data-live-tab=rooms]').click()")
                            await wait_for("!!document.querySelector('[data-live-room]')")
                            await evaluate("document.querySelector('[data-live-room]').click()")
                            await wait_for("document.querySelectorAll('[data-live-answer]').length===4")
                            await evaluate("document.querySelectorAll('[data-live-answer]')[3].click()")
                            await wait_for("!!document.querySelector('.live-option.chosen')")
                            await screenshot("interactive-room-mobile.png", 390, 844)
                            service = fixture.app[SERVICE]
                            frozen_now = [service.now() + 11]
                            service.now = lambda: frozen_now[0]
                            await service.advance()
                            await wait_for("!!document.querySelector('.live-result')")
                            assert await evaluate("document.querySelector('.live-percent').textContent==='100%'")
                            buddy = await fixture.client.get(f"/api/interactive/rooms/{room_id}", headers=fixture.headers(202))
                            assert (await buddy.json())["my_result"]["percent"] == 75
                            await screenshot("interactive-result-mobile.png", 390, 844)
                            await evaluate("document.querySelector('[data-live-leave]').click()")
                            await wait_for("!!document.querySelector('.live-rank-plays b')")
                            assert await evaluate("document.querySelector('.live-rank-plays b').textContent==='1'")
                            await evaluate("document.querySelector('[data-live-category=education]').click()")
                            await evaluate("document.querySelector('[data-live-tab=quizzes]').click()")
                            await wait_for("document.querySelectorAll('[data-live-start]').length===5")
                            await evaluate("document.querySelector('[data-live-start]').click()")
                            await wait_for("document.querySelectorAll('[data-live-answer]').length===4")
                            frozen_now[0] += 91
                            await service.advance()
                            await wait_for("document.querySelector('.live-result h2')?.textContent==='Xona to‘xtadi'")
                            await screenshot("interactive-stopped-mobile.png", 390, 844)
                            fixture.app[STATE]["rates"].clear()
                            await evaluate("document.querySelector('[data-go=profile]').click()")
                            await wait_for("!!document.querySelector('[data-go=published]')")
                            await evaluate("document.querySelector('[data-go=published]').click()")
                            await wait_for("!!document.querySelector('[data-live-delete]')")
                            await screenshot("published-mobile.png", 390, 844)
                            await evaluate("document.querySelector('[data-go=interactive-create]').click()")
                            await wait_for("!!document.querySelector('[name=live_visibility]')")
                            await evaluate("document.querySelector('[name=live_title]').value='Private invitation quiz';document.querySelector('[name=live_question]').value='Choose one';document.querySelectorAll('[name=live_option]').forEach((e,i)=>e.value=['One','Two','Three','Four'][i]);document.querySelector('[name=live_visibility]').value='private';document.querySelector('#interactive-create-form').dispatchEvent(new Event('input',{bubbles:true}))")
                            await screenshot("private-publish-mobile.png", 390, 844)
                            await evaluate("document.querySelector('#interactive-create-form').requestSubmit()")
                            await wait_for("document.querySelectorAll('[data-live-answer]').length===4")
                            private_room = await evaluate("new URLSearchParams(location.search).get('room')")
                            response = await fixture.client.post(f"/api/interactive/rooms/{private_room}/join", headers=fixture.headers(202))
                            assert response.status == 200
                            response = await fixture.client.get(f"/api/interactive/rooms/{private_room}", headers=fixture.headers(202))
                            private_quiz = (await response.json())["quiz_id"]
                            response = await fixture.client.get("/api/interactive", headers=fixture.headers(202))
                            catalogue = await response.json()
                            assert private_quiz not in [q['id'] for q in catalogue['quizzes']]
                            assert private_room not in [r['id'] for r in catalogue['rooms']]
                            await evaluate("document.querySelector('[data-go=profile]').click()")
                            await evaluate("document.querySelector('[data-go=published]').click()")
                            await wait_for("!!document.querySelector('[data-live-delete=\"" + private_quiz + "\"]')")
                            await evaluate("document.querySelector('[data-live-delete=\"" + private_quiz + "\"]').click()")
                            await wait_for("!!document.querySelector('[data-live-delete-confirm]')")
                            await screenshot("published-remove-mobile.png", 390, 844)
                            await evaluate("document.querySelector('[data-live-delete-confirm]').click()")
                            await wait_for("!document.querySelector('[data-live-delete=\"" + private_quiz + "\"]')")
                            response = await fixture.client.get(f"/api/interactive/rooms/{private_room}", headers=fixture.headers(202))
                            assert (await response.json())["reason"] == "deleted"
                            with patch("services.support_inbox.ADMIN_IDS", [999]):
                                await evaluate("window.Telegram.WebApp.initData=" + json.dumps(signed(999)) + ";document.querySelector('[data-go=profile]').click()")
                                await wait_for("!!document.querySelector('[data-go=admin-support]')")
                                await evaluate("document.querySelector('[data-go=admin-support]').click()")
                                await wait_for("!!document.querySelector('[data-support-thread=\"101\"]')")
                                await screenshot("admin-inbox-mobile.png", 390, 844)
                                await evaluate("document.querySelector('[data-support-thread=\"101\"]').click()")
                                await wait_for("!!document.querySelector('#support-messages .support-bubble')")
                                await evaluate("document.querySelector('[name=support_message]').value='Administrator web reply';document.querySelector('#support-form').requestSubmit()")
                                await wait_for("document.querySelector('#support-messages')?.textContent.includes('Administrator web reply')")
                                await evaluate("window.Telegram.WebApp.initData=" + json.dumps(signed()) + ";document.querySelector('[data-go=profile]').click()")
                                await wait_for("document.querySelector('[data-go=support]')?.textContent.includes('(1)')")
                                assert not await evaluate("!!document.querySelector('[data-go=admin-support]')")
                                await evaluate("document.querySelector('[data-go=support]').click()")
                                await wait_for("document.querySelector('#support-messages')?.textContent.includes('Administrator web reply')")
                                await evaluate("document.querySelector('[name=support_message]').value='Unsent draft stays here'")
                                response = await fixture.client.post("/api/admin/support/101/messages", json={"message": "Second web reply"}, headers=fixture.headers(999))
                                assert response.status == 200
                                await wait_for("document.querySelector('#support-messages')?.textContent.includes('Second web reply')")
                                assert await evaluate("document.querySelector('[name=support_message]').value==='Unsent draft stays here'")
                                await screenshot("support-conversation-mobile.png", 390, 844)
                            await evaluate("document.querySelector('[data-go=file]').click(); document.documentElement.dataset.theme='dark'")
                            await screenshot("file-dark.png", 390, 844)
                            await evaluate("document.documentElement.dataset.theme='light'")
                            await screenshot("file-desktop.png", 1100, 950)
                            expired = signed(timestamp=int(time.time()) - 100000)
                            await evaluate("window.Telegram.WebApp.initData=" + json.dumps(expired) + ";document.querySelector('[data-go=profile]').click()")
                            await evaluate("document.querySelector('[data-go=quizzes]').click()")
                            await wait_for("!!document.querySelector('[data-reopen-app]')")
                            assert "Sessiya muddati tugadi" in await evaluate("document.querySelector('.error').textContent")
                            assert not await evaluate("!!document.querySelector('.empty')"), "An auth failure must not claim that quizzes are empty"
                            await evaluate("document.querySelector('[data-reopen-app]').click()")
                            assert await evaluate("window.lastTelegramLink.endsWith('start=webapp')")
                            await screenshot("session-recovery-mobile.png", 390, 844)
                            await evaluate("window.Telegram.WebApp.initData=" + json.dumps(signed()) + ";document.querySelector('[data-reload-quizzes]').click()")
                            await wait_for("!!document.querySelector('[data-quiz=\"" + str(bot_quiz_id) + "\"]')")
                            wrong = signed(token="987654321:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
                            await evaluate("window.Telegram.WebApp.initData=" + json.dumps(wrong) + ";document.querySelector('[data-go=profile]').click()")
                            await evaluate("document.querySelector('[data-go=quizzes]').click()")
                            await wait_for("!!document.querySelector('[data-reopen-app]')")
                            assert "Telegram kirishini tasdiqlab bo‘lmadi" in await evaluate("document.querySelector('.error').textContent")
                            assert "Sessiya muddati tugadi" not in await evaluate("document.querySelector('.error').textContent")
                            await cdp("Page.addScriptToEvaluateOnNewDocument", {"source": "window.Telegram=undefined;"})
                            await cdp("Page.navigate", {"url": url + '#' + urlencode({"tgWebAppData": signed()})})
                            await cdp("Page.reload", {"ignoreCache": True})
                            await wait_for("typeof window.Telegram === 'undefined' && document.querySelector('#connection')?.hidden")
                            await evaluate("document.querySelector('[data-go=profile]').click()")
                            await wait_for("!!document.querySelector('[data-go=support]')")
                            await evaluate("document.querySelector('[data-go=quizzes]').click()")
                            await wait_for("!!document.querySelector('[data-quiz=\"" + str(bot_quiz_id) + "\"]')")
                            assert not exceptions, exceptions
                            print(json.dumps({"status": "passed", "screenshots": str(output), "checks": ["5 footer tabs", "320px and 390px layouts", "AI topic to saved result", "Telegram quiz link", "manual quiz and draft cleanup", "profile editing", "portrait crop selection and saved pixels", "fit entire long photo", "avatar upload and reload", "PNG JPEG WebP avatars", "profile help and admin delivery", "private publishing and invite-only room", "author-only removal and room stop", "published quizzes in profile", "web admin inbox and replies", "unread reply count", "incoming replies preserve draft", "language change", "premium plans and link", "referral link", "interactive categories", "weighted test publishing", "host-independent shared room", "affinity percentage", "creator ranking", "3 unanswered question stop", "dark theme", "desktop", "no JS exceptions"]}), flush=True)
            finally:
                if port and browser.poll() is None:
                    try:
                        async with ClientSession() as cleanup_session:
                            async with cleanup_session.get(f"http://127.0.0.1:{port}/json/version", timeout=3) as response:
                                metadata = await response.json()
                            async with cleanup_session.ws_connect(metadata["webSocketDebuggerUrl"]) as cleanup_ws:
                                await cleanup_ws.send_json({"id": 1, "method": "Browser.close"})
                            for _ in range(30):
                                if browser.poll() is not None:
                                    break
                                await asyncio.sleep(.1)
                    except Exception:
                        pass
                if browser.poll() is None:
                    browser.terminate()
                try:
                    browser.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    browser.kill()
                    browser.wait()
                browser_log.close()
    finally:
        await fixture.asyncTearDown()


if __name__ == "__main__":
    asyncio.run(run())
