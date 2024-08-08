import discord as dc
from discord.ext import tasks
from secret import TEXTCHANNEL_ID, ERRORCHANNEL_ID
import asyncio as aio
from bs4 import BeautifulSoup as bs
import datetime
from datetime import datetime as dt, timezone as tz, timedelta as td
import os
import json
import urllib3
uo = urllib3.PoolManager().request

BOT_NAME = "Custumber Notice Bot"
BOT_VERSION = "5.6d"

from companies import Company
Citybus = Company([], ['no', 'title', 'date', 'route'], 'yellow', "Citybus", "bravobus", 'http://mobile.bravobus.com.hk/pdf/{target}.pdf')
KMBus = Company([], ['url', 'number', 'title', 'route'], 'red', "KMB", "KMBLWB", 'https://search.kmb.hk/KMBWebSite/AnnouncementPicture.ashx?url={target}.pdf')
NLBus = Company([], ['no', 'title', 'date'], 'green', "NLB", "NLB", 'https://www.nlb.com.hk/news/detail/{target}')

from enum import Enum
class Mode(Enum):
    added = 1
    amended = 0
    removed = -1
   
batch_threshold = {
    Mode.added: 10,
    Mode.amended: 10,
    Mode.removed: 20
}


def move_old_info(o, n):
    os.remove(o)
    os.rename(n, o)
    return


def create_set_from_json(notice_file, company) -> tuple[dict, set]:
    data = open(notice_file, encoding="utf-8", mode='r')
    notices = json.loads(data.read().encode('utf-8'))
    data.close()
    notice_set = set()
    for _ in notices['data']:
        notice_set.add(_[company.sort_criteria[0]])
    # for _ in range(depth):
        # os.chdir(os.pardir)
    return notices, notice_set


def check_notices_info(notices, notice_json, company):
    changed_contents = list()
    for notice in notices:
        for info in notice_json:
            if info[company.sort_criteria[0]] == notice:
                changed_contents.append([info[criterion] for criterion in company.sort_criteria])
                break
    changed_contents.sort()
    return changed_contents

def check_for_changed(notices, old_json, new_json, company:Company):
    changed_contents = list()
    criteria = company.sort_criteria
    for notice in notices:
        for info in old_json:
            if info[criteria[0]] == notice:
                temp_old = info[criteria[1]], info[criteria[2]] if len(criteria) > 2 else info[criteria[1]]
                break
        for info in new_json:
            if info[criteria[0]] == notice:
                temp_new = info[criteria[1]], info[criteria[2]]
                break
        for digit in range(2):
            if temp_old[digit] != temp_new[digit]:
                changed_contents.append([notice, temp_old, temp_new])
                break
    changed_contents.sort()
    return changed_contents

async def notify(channel: dc.TextChannel, error_channel: dc.TextChannel, mode: Mode, title: str, link: str, company:Company, rtno:str = ''):
    message = company.circles(10)
    message += f'\nNotice {mode.name}: {title}'
    if len(rtno) > 0: message += f'\tRoute: {rtno}'
    if mode != Mode.removed: #if removed, link is meaningless
        message += f"\n{link}" + ('\n@everyone' if company == NLBus else '')
    await channel.send(message)

async def batch_notify(channel: dc.TextChannel, error_channel: dc.TextChannel, mode: Mode, notices: list, company:Company):
    message = company.squares(10)
    message += f'\nNotices {mode.name} ({len(notices)} in total):\n'
    back_message = message
    for notice in notices:
        message += notice[company.sort_criteria.index("title")] + f'\t{company.link.format(target=notice[0])}\n'
    try:
        await channel.send(message)
    except Exception as e:
        back_message += f'Message sending fails, because: {e} The following text is what I tried to send.'
        await error_channel.send(back_message)
        m_txt = f'Message {company.displayname}.txt'
        attempt_message = open(m_txt, mode = 'x', encoding='utf-8')
        attempt_message.write(message)
        attempt_message.close()
        attempt_message = open(m_txt, mode='rb')
        await error_channel.send('Attempt Message', file=dc.File(attempt_message))
        attempt_message.close(); attempt_message = open(m_txt, mode = 'rb')
        await channel.send(f'A message was to big to be sent, so it became a file instead in {error_channel.name}', file=dc.File(attempt_message))
        attempt_message.close()
        os.remove(m_txt)
        

async def write_txt_and_notify(channel: dc.TextChannel, error_channel: dc.TextChannel, t, removed, tier_old, added, tier_new, changed, updates_file, company:Company):
    txt = open(updates_file, encoding="utf-8")
    ori = txt.read()
    txt.close()

    txt = open(updates_file, 'w', encoding="utf-8")
    txt.write(f'{company.filename} notices update as of {t[6:8]}/{t[4:6]}/{t[0:4]}, {t[8:10]}:{t[10:12]}:{t[12:14]}\n')
    txt.write(f'Comparing notices at {tier_old[6:8]}/{tier_old[4:6]}/{tier_old[0:4]}, {tier_old[8:10]}:{tier_old[10:12]}:{tier_old[12:14]}')
    txt.write(f' and {tier_new[6:8]}/{tier_new[4:6]}/{tier_new[0:4]}, {tier_new[8:10]}:{tier_new[10:12]}:{tier_new[12:14]}\n\n')

    #As an "notice" in removed/added/changed is a list of three, and need to find the "title"
    txt.write('Removed notice(s):\n')
    if len(removed) > batch_threshold[Mode.removed]:
        txt.write('\n'.join(str(notice) for notice in removed) + '\n')
        await batch_notify(channel, error_channel, Mode.removed, removed, company)
    else:
      for notice in removed:
        txt.write(f'{str(notice)}\n')
        title = notice[company.sort_criteria.index('title')]
        link = company.link.format(target=notice[0])
        await notify(channel, error_channel, Mode.removed, title, link, company)

    txt.write('\nAdded notice(s):\n')
    if len(added) > batch_threshold[Mode.added]:
        txt.write('\n'.join(str(notice) for notice in added) + '\n')
        await batch_notify(channel, error_channel, Mode.added, added, company)
    else:
      for notice in added:
        txt.write(f'{str(notice)}\n')
        title = notice[company.sort_criteria.index('title')]
        link = company.link.format(target=notice[0])
        await notify(channel, error_channel, Mode.added, title, link, company)

    txt.write('\nAmended notice(s):\n')
    if len(changed) > batch_threshold[Mode.amended]:
        txt.write('\n'.join(str(notice) for notice in changed) + '\n')
        await batch_notify(channel, error_channel, Mode.amended, changed, company)
    else:
      for notice in changed:
        txt.write(f'{str(notice)}\n')
        title = notice[company.sort_criteria.index('title')]
        link = company.link.format(target=notice[0])
        await notify(channel, error_channel, Mode.amended, title, link, company)

    if len(removed) + len(added) + len(changed) == 0 and tier_new[0:10] != tier_old[0:10]:
        message = company.circles(10)
        message += f"\nCNB V{BOT_VERSION}: No notice updates for {company.filename}"
        try:
            await channel.send(message)
            #manually set json to one hour earlier and test using aio run of the coroutine instead to try
        except Exception as e:
            print("Failed:", e)
    txt.write('\n---------------------------------------\n')
    txt.write(ori)
    txt.close()
    return

import threading
async def download_pdf_and_notify(textchannel: dc.TextChannel, error_channel: dc.TextChannel, notices, company:Company, mode:Mode):
    if company == NLBus: return
    field_to_look = company.sort_criteria.index('title') #2 if company == 'KMBLWB' else 1
    if len(notices) > batch_threshold[mode]:
        print(f'Batch processing {len(notices)} notices:')
        print('\t'.join(n[field_to_look] for n in notices))
        def download(company: Company, notice, notice_loaded: dict, num: int):
            url = company.link.format(target = notice[0])
            path = f'{company.filename}{os.sep}notices{os.sep}{notice[0]}.pdf'
            with open(path, 'wb') as f:
                f.write(uo('GET', url, preload_content = False).read())
            notice_loaded[num] = dc.File(open(path, 'rb'), filename=f'{notice[0]}.pdf')
        def unravel_dict(d: dict, start, end):
            l = list()
            for _ in range(start, end):
                l.append(d[_])
            return l
        notice_files = dict()
        notice_threads = [threading.Thread(target=download, args=(company, notices[e],notice_files,e))
                          for e in range(len(notices))]
        for t in notice_threads: t.start()
        joined = 0; notified = 0
        joinLock = threading.Lock(); notifiedlock = threading.Lock()
        for t in notice_threads:
            t.join()
            with joinLock: joined += 1
            if joined - notified >= 10 or joined == len(notices):
                end = len(notices) if joined == len(notices) else notified + 10
                batch_files = unravel_dict(notice_files, notified, end)
                from math import ceil
                message = f'{company.squares(3)} Notices batch {notified // 10 + 1} of {ceil(len(notices) / 10)}:\n\n'
                message += '\n'.join(f'{notices[e][field_to_look]}: {notices[e][0]}' for e in range(notified, end))
                await textchannel.send(message, files=batch_files)
                with notifiedlock: notified += 10
    else:
      for notice in notices:
        url = company.link.format(target = notice[0])
        path = f'{company.filename}{os.sep}notices{os.sep}{notice[0]}.pdf'
        print(f'processing {notice[field_to_look]}')
        with open(path, 'wb') as f:
            f.write(uo('GET', url, preload_content=False).read())
        print(f'finished writing {notice[field_to_look]}')
        await textchannel.send(notice[field_to_look], file=dc.File(open(path, 'rb'), filename=f'{notice[0]}.pdf'))
        print(f'finished notifying {notice[field_to_look]}')
    return

def find_bravo_routes():
    addr = 'https://rt.data.gov.hk/v2/transport/citybus/route/ctb'
    data = json.loads(uo('GET', addr).data.decode('utf-8'))
    return [d['route'] for d in data["data"]]

def find_kmb_routes():
    addr = 'https://data.etabus.gov.hk/v1/transport/kmb/route/'
    data = json.loads(uo('GET', addr).data.decode('utf-8'))
    return list({d['route'] for d in data['data']})

def find_nlb_routes():
    addr = 'https://rt.data.gov.hk/v2/transport/nlb/route.php?action=list'
    data = json.loads(uo('GET', addr).data.decode('utf-8'))
    return list({d['routeNo'] for d in data['routes']})

def find_bravo_notice_one(routes, parts: slice, notice_dict):
    
    for rt in routes[parts]:
        notice_link = f'http://mobile.citybus.com.hk/nwp3/getnotice.php?id={rt}'
        datas = bs(uo('GET', notice_link, timeout=250).data, 'html.parser').find_all('tr', style="background-color:'#ffffff'; cursor: pointer;")
        for data in datas:
            #notice_no = data.select_one('td').get('onclick').strip(r'javascript:window.open();').strip(r"'http://mobile.bravobus.com.hk/pdf/.pdf'") #The html source still use "bravobus"
            notice_no = data.select_one('td').get('onclick').strip(r'javascript:window.open();').strip(r"'http://mobile.citybus.com.hk/pdf/.pdf'") #fix in V4.2
            
            for _ in data.findAll('td', {'valign': 'middle', 'colspan': '2'}):
                    entry = _.get_text()
                    notice_dict[(notice_no, rt)] = [entry[:11].strip(), entry[11:].strip()]
    print(f'Thread {parts.start} ends', end='. ') #for debug

async def find_bravo_notice(threads):
    notice_dict = dict()
    threads_list = [
        threading.Thread(target=find_bravo_notice_one, args=(Citybus.routeslist, slice(i,-1,threads), notice_dict))
        for i in range(threads)
    ]
    for thread in threads_list:
        thread.start()
    for thread in threads_list:
        thread.join()
       
    return notice_dict

def find_kmb_notice_one(routes, parts: slice, notice_dict):
    for rt in routes[parts]:
        notice_link = f'http://search.kmb.hk/KMBWebSite/Function/FunctionRequest.ashx?action=getAnnounce&route={rt}&bound=1'
        rt_notices = json.loads(uo('GET', notice_link, timeout=250).data.decode('utf-8'))['data']
        for rt_notice in rt_notices:
            refno, url = rt_notice['kpi_referenceno'], rt_notice['kpi_noticeimageurl']
            #with notice_dict_lock:
            try:
                    if url[-3:] == 'pdf' and refno[:2] != 'MP':
                        notice_dict[(url[:-4], rt)] = [refno, rt_notice['kpi_title_chi']]
            except TypeError:
                    None
    print(f'Thread {parts.start} ends', end='. ') #for debug

async def find_kmb_notice(threads):
    notice_dict = dict()
    threads_list = [
        threading.Thread(target=find_kmb_notice_one, args=(KMBus.routeslist, slice(i,-1,threads), notice_dict))
        for i in range(threads)
    ]
    for thread in threads_list:
        thread.start()
    for thread in threads_list:
        thread.join()
    
    return notice_dict

import re
async def find_nlb_notice(threads):
    notice_dict = dict()
    noticepage = bs(uo('GET', 'https://www.nlb.com.hk/news', timeout=250).data, 'html.parser')
    notices = noticepage.find_all('a', href=re.compile("news/detail/.+"), class_="")
    noticedates = noticepage.body.div(class_='main')[0].find_all('a', href="#", onclick="return false;")
    for notice in zip(notices, noticedates):
        notice_dict[int(notice[0].get('href').strip('news/detail'))] = [notice[0].get_text(), notice[1].get_text()]
    return notice_dict

Citybus.findnotice = find_bravo_notice
KMBus.findnotice = find_kmb_notice
NLBus.findnotice = find_nlb_notice

async def sort_notice(threads, company:Company):
    try: 
        notices = await company.findnotice(threads)
    except TimeoutError:
        print('\033[35;1mTimeout\033[0m')
    else:
      notice_list = list()
      for _ in notices:
        if company == Citybus:
            notice_list.append([_[0], notices[_][1], notices [_][0], _[1]])
        elif company == KMBus:
            notice_list.append([_[0], notices[_][0], notices [_][1], _[1]])
        elif company == NLBus:
            notice_list.append([_, notices[_][0], notices[_][1]])
      notice_list.sort()
    notices = list()
    for notice in notice_list:
        notices.append(dict(zip(company.sort_criteria, notice)))
    return notices

async def write_json(old_file, new_file, t, threads, company:Company) -> tuple[dict, set]:
    print('working')
    notices = await sort_notice(threads, company)
    dictionary = {'data': notices, 'time': t}
    json_object = json.dumps(dictionary, indent=2)
    print('json')
    move_old_info(old_file, new_file)
    with open(new_file, 'w', encoding='utf-8') as file:
        file.write(json_object)
    notice_set = set()
    for _ in notices:
        notice_set.add(_[company.sort_criteria[0]])
    return dictionary, notice_set

async def fetch_notices(textchannel, error_channel, t, company:Company, thread_count = 1):
    old_file = f'{company.filename}{os.sep}data{os.sep}{company.filename}_old.json'
    new_file = f'{company.filename}{os.sep}data{os.sep}{company.filename}_new.json'
    updates_file = f'{company.filename}{os.sep}data{os.sep}{company.filename}_notices_update.txt'

    new_json, new_set = await write_json(old_file, new_file, t, thread_count, company)
    old_json, old_set = create_set_from_json(old_file, company)

    removed_list = check_notices_info(company.removed_buffer - new_set, old_json['data'], company)
    company.removed_buffer = old_set - new_set
    if len(company.removed_buffer) > 0:
        print(f'Buffered for {company.displayname}', end=": ")
        print("\t".join(company.removed_buffer))
    added_list = check_notices_info(new_set - old_set, new_json['data'], company)
    changed_list = check_for_changed(old_set & new_set, old_json['data'], new_json['data'], company)
    await write_txt_and_notify(textchannel, error_channel, t, removed_list, old_json['time'], added_list, new_json['time'], changed_list, updates_file, company)
    await aio.gather(download_pdf_and_notify(textchannel, error_channel, added_list, company, Mode.added),
                     download_pdf_and_notify(textchannel, error_channel, changed_list, company, Mode.amended))
    return

async def probe_(textchannel: dc.TextChannel, error_channel: dc.TextChannel, company:Company, thread: int = 1):
    t = dt.now(tz(td(hours=+8))).strftime("%Y%m%d%H%M%S")
    print(f'Probe for {company.displayname}...')
    if t[8:12] == '1158' and company == KMBus:
        print('KMB 11:58 pause')
        return
    await fetch_notices(textchannel, error_channel, t, company, thread)
    print(f'Probe for {company.displayname} stopped.')

async def enquire_route(channel: dc.TextChannel, route : str):
    await channel.send(f"CNB V{BOT_VERSION}: Enquires route {route}")
    results_message = f'bravobus {route}'
    results_message += '' if route in Citybus.routeslist else ' not'
    results_message += ' found. '
    results_message += ':yellow_circle:' if route in Citybus.routeslist else ':negative_squared_cross_mark:'
    results_message += f'\nKMB {route}'
    results_message += ' found. :red_circle:' if route in KMBus.routeslist else ' not found. :negative_squared_cross_mark:'
    await channel.send(results_message)

def initialize_file(company : Company):
    subdirs = ['data','notices']
    for subdir in subdirs:
        needfile = os.curdir + os.sep + company.filename + os.sep + subdir
        if not os.path.exists(needfile):
            os.makedirs(needfile)
    template = json.dumps({'data': [], 'time': ''})
    datafiles = {'_old.json': template,
                 '_new.json': template,
                 '_notices_update.txt': ''}
    for file in datafiles:
        needfile = company.filename + os.sep + 'data' + os.sep + company.filename + file
        f = open(needfile, ('r' if os.path.exists(needfile) else 'x'))
        if f.mode=='x':
            f.write(datafiles[file])
        print(f'{needfile} initialized.')
        f.close()

import atexit
def run_discord_bot():
    from secret import TOKEN
    bot_intent = dc.Intents.default()
    bot_intent.message_content = True
    client = dc.Client(intents=bot_intent)

    @atexit.register
    def goodbye():
        aio.run(client.close())
        print('Bye!')
    
    
    @tasks.loop(seconds=15)
    async def probes_(text_channel, error_channel):
        await aio.gather(
            probe_(text_channel, error_channel, Citybus, 4),
            probe_(text_channel, error_channel, KMBus, 16),
            probe_(text_channel, error_channel, NLBus) # NLBus notices are not routewise
        )

    @tasks.loop(time=datetime.time(hour=4, minute=55, tzinfo=tz(td(hours=+8))))
    async def update_bravo_routes(textchannel: dc.TextChannel, error_channel):
        print('Searching Citybus routes')
        in_route_list = find_bravo_routes()
        new_route_set = set(in_route_list) - set (Citybus.routeslist)
        if len(new_route_set) > 0:
            message = ":yellow_circle:" * 5
            message += f"CNB V{BOT_VERSION}: New route for bravovus: {new_route_set}\n@everyone"
            await textchannel.send(message)
            Citybus.routeslist = in_route_list

    @tasks.loop(time=datetime.time(hour=4, minute=55, tzinfo=tz(td(hours=+8))))
    async def update_kmb_routes(textchannel: dc.TextChannel, error_channel):
        print(f'Searching KMB routes')
        in_route_list = find_kmb_routes()
        new_route_set = set(in_route_list) - set (KMBus.routeslist)
        if len(new_route_set) > 0:
            message = ":red_circle:" * 5
            message += f"CNB V{BOT_VERSION}: New route for KMBLWB: {new_route_set}\n@everyone"
            await textchannel.send(message)
            KMBus.routeslist = in_route_list

    @client.event
    async def on_ready():
        initialize_file(Citybus)
        initialize_file(KMBus)
        initialize_file(NLBus)
        print(f'{BOT_NAME} Ready, Version {BOT_VERSION}')
        Citybus.routeslist = find_bravo_routes()
        KMBus.routeslist = find_kmb_routes()
        NLBus.routeslist = find_nlb_routes(); print(f'{NLBus.routeslist = }')
        # guild = client.guilds[0]
        text_channel = client.get_channel(TEXTCHANNEL_ID)
        error_channel = client.get_channel(ERRORCHANNEL_ID)
        probes_.start(text_channel, error_channel)
        update_bravo_routes.start(text_channel, error_channel)
        update_kmb_routes.start(text_channel, error_channel)
            
    @client.event
    async def on_message(message: dc.Message):
        print(message.content)
        if message.author == client.user:
            return
        if message.content[:5].lower() == "!cnb ":
            actual_content = message.content[5:]
            if actual_content[:5].lower() == "route":
                await enquire_route(message.channel, actual_content[::])
        else:
            commands_list = message.content.split(' ')
            if 'route' in commands_list:
                await enquire_route(message.channel, commands_list[commands_list.index('route') + 1])

    client.run(TOKEN)

        
if __name__ == "__main__":
    run_discord_bot()
