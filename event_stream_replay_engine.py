import argparse
import hashlib
import json
import time
from collections import deque
from pathlib import Path

class EventStore:
    def __init__(self,path):
        self.path=Path(path)
        self.path.parent.mkdir(parents=True,exist_ok=True)
    def append(self,event_type,aggregate,payload):
        events=self.read()
        sequence=len(events)+1
        previous=events[-1]["hash"] if events else "0"*64
        event={"sequence":sequence,"timestamp":time.time(),"type":event_type,"aggregate":aggregate,"payload":payload,"previous_hash":previous}
        event["hash"]=self.digest(event)
        with self.path.open("a",encoding="utf-8") as f:f.write(json.dumps(event,separators=(",",":"))+"\n")
        return event
    def digest(self,event):
        raw=json.dumps({k:event[k] for k in ["sequence","timestamp","type","aggregate","payload","previous_hash"]},sort_keys=True,separators=(",",":")).encode()
        return hashlib.sha256(raw).hexdigest()
    def read(self):
        if not self.path.exists():return []
        events=[]
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if line.strip():events.append(json.loads(line))
        return events
    def verify(self):
        events=self.read()
        previous="0"*64
        for index,event in enumerate(events,1):
            if event.get("sequence")!=index:return False,index,"sequence_mismatch"
            if event.get("previous_hash")!=previous:return False,index,"previous_hash_mismatch"
            if event.get("hash")!=self.digest(event):return False,index,"hash_mismatch"
            previous=event["hash"]
        return True,len(events),"ok"

class ReplayEngine:
    def __init__(self):
        self.handlers={"AccountCreated":self.account_created,"MoneyDeposited":self.money_deposited,"MoneyWithdrawn":self.money_withdrawn,"ProductAdded":self.product_added,"ProductRemoved":self.product_removed,"UserUpdated":self.user_updated}
    def initial_state(self):
        return {"accounts":{},"products":{},"users":{}}
    def account_created(self,state,event):
        a=str(event["aggregate"]);state["accounts"].setdefault(a,{"balance":0.0})
    def money_deposited(self,state,event):
        a=str(event["aggregate"]);state["accounts"].setdefault(a,{"balance":0.0});state["accounts"][a]["balance"]=round(state["accounts"][a]["balance"]+float(event["payload"].get("amount",0)),2)
    def money_withdrawn(self,state,event):
        a=str(event["aggregate"]);state["accounts"].setdefault(a,{"balance":0.0});state["accounts"][a]["balance"]=round(state["accounts"][a]["balance"]-float(event["payload"].get("amount",0)),2)
    def product_added(self,state,event):
        p=str(event["aggregate"]);state["products"][p]=dict(event["payload"])
    def product_removed(self,state,event):
        state["products"].pop(str(event["aggregate"]),None)
    def user_updated(self,state,event):
        u=str(event["aggregate"]);state["users"].setdefault(u,{}).update(event["payload"])
    def apply(self,state,event):
        handler=self.handlers.get(event["type"])
        if handler:handler(state,event)
    def replay(self,events,end=None):
        state=self.initial_state()
        selected=events if end is None else events[:max(0,end)]
        for event in selected:self.apply(state,event)
        return state
    def snapshot(self,events,end=None):
        state=self.replay(events,end)
        count=len(events) if end is None else min(max(0,end),len(events))
        return {"event_count":count,"state":state}
    def compare(self,events,first,second):
        a=self.replay(events,first)
        b=self.replay(events,second)
        return {"first_event":first,"second_event":second,"first_state":a,"second_state":b}

def demo_store(path):
    store=EventStore(path)
    if store.read():return
    store.append("AccountCreated","acct-1",{"owner":"demo"})
    store.append("MoneyDeposited","acct-1",{"amount":1000})
    store.append("MoneyWithdrawn","acct-1",{"amount":250})
    store.append("ProductAdded","prod-1",{"name":"Laptop","stock":5})
    store.append("ProductRemoved","prod-1",{})
    store.append("UserUpdated","user-1",{"role":"engineer"})

def print_state(state):
    print(json.dumps(state,indent=2,sort_keys=True))

def main():
    parser=argparse.ArgumentParser(prog="event_stream_replay_engine")
    sub=parser.add_subparsers(dest="command")
    demo=sub.add_parser("demo")
    demo.add_argument("--store",default="events.jsonl")
    demo.add_argument("--json",dest="json_path",default="")
    append_cmd=sub.add_parser("append")
    append_cmd.add_argument("--store",default="events.jsonl")
    append_cmd.add_argument("--type",required=True)
    append_cmd.add_argument("--aggregate",required=True)
    append_cmd.add_argument("--payload",default="{}")
    append_cmd.add_argument("--json",dest="json_path",default="")
    replay=sub.add_parser("replay")
    replay.add_argument("--store",default="events.jsonl")
    replay.add_argument("--end",type=int,default=0)
    replay.add_argument("--json",dest="json_path",default="")
    verify=sub.add_parser("verify")
    verify.add_argument("--store",default="events.jsonl")
    verify.add_argument("--json",dest="json_path",default="")
    history=sub.add_parser("history")
    history.add_argument("--store",default="events.jsonl")
    history.add_argument("--json",dest="json_path",default="")
    snapshot=sub.add_parser("snapshot")
    snapshot.add_argument("--store",default="events.jsonl")
    snapshot.add_argument("--end",type=int,default=0)
    snapshot.add_argument("--json",dest="json_path",default="")
    compare=sub.add_parser("compare")
    compare.add_argument("--store",default="events.jsonl")
    compare.add_argument("--first",type=int,required=True)
    compare.add_argument("--second",type=int,required=True)
    compare.add_argument("--json",dest="json_path",default="")
    args=parser.parse_args()
    if not args.command:
        args.command="demo"
        args.store="events.jsonl"
        args.json_path=""
    try:
        if args.command=="demo":
            store=EventStore(args.store)
            demo_store(args.store)
            events=store.read()
            valid,count,reason=store.verify()
            state=ReplayEngine().replay(events)
            report={"integrity":valid,"verified_events":count,"reason":reason,"event_count":len(events),"state":state}
            print("EVENT STREAM REPLAY ENGINE")
            print("="*60)
            print("Integrity:",valid)
            print("Verified:",count)
            print("Event count:",len(events))
            print("State:")
            print_state(state)
        elif args.command=="append":
            payload=json.loads(args.payload)
            event=EventStore(args.store).append(args.type,args.aggregate,payload)
            report=event
            print(json.dumps(event,indent=2))
        elif args.command=="replay":
            events=EventStore(args.store).read()
            end=args.end if args.end>0 else None
            report=ReplayEngine().snapshot(events,end)
            print_state(report["state"])
        elif args.command=="verify":
            valid,count,reason=EventStore(args.store).verify()
            report={"integrity":valid,"verified_events":count,"reason":reason}
            print(json.dumps(report,indent=2))
        elif args.command=="history":
            events=EventStore(args.store).read()
            report={"events":events}
            for event in events:print(f"{event['sequence']} {event['type']} {event['aggregate']}")
        elif args.command=="snapshot":
            events=EventStore(args.store).read()
            end=args.end if args.end>0 else None
            report=ReplayEngine().snapshot(events,end)
            print_state(report)
        elif args.command=="compare":
            events=EventStore(args.store).read()
            report=ReplayEngine().compare(events,args.first,args.second)
            print(json.dumps(report,indent=2,sort_keys=True))
        if args.json_path:
            Path(args.json_path).expanduser().resolve().write_text(json.dumps(report,indent=2,sort_keys=True),encoding="utf-8")
            print(f"Saved: {Path(args.json_path).expanduser().resolve()}")
    except Exception as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1)

if __name__=="__main__":main()
