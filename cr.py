#import liberary
from flask import Flask,render_template,request,send_file,make_response
import pandas as pd

#instance of an app
app=Flask(__name__)

@app.route('/' )
def selection():
    return render_template("selection.html")

@app.route('/daily')
def daily_upload():
    return render_template("daily_up.html")

@app.route('/monthly')
def monthly_upload():
    return render_template("monthly_up.html")

@app.route('/daily_app',methods=['POST'])
def success():

    global odf
    global uppcl_df
    global rural


    import pandas as pd
    import datetime as dt
    import numpy as np
    f=request.files['daily_export']
    of=pd.read_excel(f)


    ff=pd.read_excel('Feeders.xlsx')

    of.drop(columns=['SUBSTATION','SUBSTATION DESC','OUTAGE ID','STATUS','TOTAL OUTAGE TIME','AGGREGATE TIME',"REASON",'REASON DESCRIPTION','REMARKS','LOCATION','STAND BY FEEDER','SUBTOTAL AGG.',"FEEDER TYPE"],inplace=True)

    of["FEEDER DESC"]=of["FEEDER DESC"].astype(str)
    of.drop(list(of[of["FEEDER DESC"]=="nan"].index),inplace=True)

    ff.drop(columns=["SI. No."],inplace=True)

    off=pd.merge(of,ff,on="FEEDER ID",how="left")


    off.drop(columns=["FEEDER DESC_x"],inplace=True)
    off.rename(columns={"FEEDER DESC_y":"FEEDER DESC"},inplace=True)


    off.dropna(how="any",axis=0,inplace=True)

    df=off[:]

    for i in df[df["END DATE"]>df["START DATE"]].index:
        df["END TIME"][i]=dt.time(hour=0,minute=0,second=0)



    df["START DATE"]=df["START DATE"].astype('str')
    df["END DATE"]=df["END DATE"].astype('str')
    df["START TIME"]=df["START TIME"].astype('str')
    df["END TIME"]=df["END TIME"].astype('str')
    df["temp"]=" "
    df["START TIME"]=df["START DATE"]+df["temp"]+df["START TIME"]
    df["END TIME"]=df["END DATE"]+df["temp"]+df["END TIME"]
    df.drop(columns=["START DATE","END DATE","temp"],inplace=True)
    df["START TIME"]=pd.to_datetime(df["START TIME"])
    df["END TIME"]=pd.to_datetime(df["END TIME"])

    a=df.groupby("FEEDER DESC")
    b=list(df["FEEDER DESC"].unique())

    for i in b:
        x=0
        m=a.get_group(i).sort_values(by="START TIME")
        for j,k in m.iterrows():
            if(x>0):
                if(df["START TIME"][j]<df["END TIME"][m]):
                    df["START TIME"][j]=df["END TIME"][m]
                if(df["END TIME"][j]<df["START TIME"][j]):
                    df["END TIME"][j]=df["START TIME"][j]
            
               
          
                
                
            m=j
            x+=1
        
    df["TOTAL OUTAGE TIME"]=df["END TIME"]-df["START TIME"]

    lst=[]
    for i,j in df.iterrows():
        if df.loc[i]["TOTAL OUTAGE TIME"]==pd.Timedelta("0 days"):
            lst.append(i)
        
    df.drop(lst,inplace=True)

    xf=(df.pivot_table(index="FEEDER ID",columns="OUTAGE TYPE",values="TOTAL OUTAGE TIME",aggfunc="sum",margins=True)).fillna(value="0")
    rf=pd.read_excel("Rural.xlsx")
    rf.dropna(axis=1,inplace=True,how="all")
    rf.dropna(axis=0,inplace=True,how="any")
    rur_data=pd.merge(rf,xf,on="FEEDER ID",how="left").fillna(pd.Timedelta("0 days 00:00:01"))

    rural=rur_data[["Feeder ","All"]]

    rural["SUPPLY HOURS"]=pd.Timedelta("1 days")-rural["All"]

    rural.drop(columns=["All"],inplace=True)

    

    up_df=df[:]
    upp_df=up_df[(up_df["FEEDER TYPE"]!="Rural")]
    drop_list=upp_df[upp_df["OUTAGE TYPE"]=="SHUTDOWN"].index
    up_df.drop(drop_list,axis=0,inplace=True)



    mod_df=(up_df.pivot_table(index="FEEDER ID",columns="OUTAGE TYPE",values="TOTAL OUTAGE TIME",aggfunc="sum",margins=True))
    nf=pd.merge(ff,mod_df,on="FEEDER ID",how="left")
    updf=nf.pivot_table(index="FEEDER TYPE",aggfunc="sum",values="All",margins=True).fillna(value=pd.Timedelta("0 days"))
    updf.sort_values(by="FEEDER TYPE",inplace=True)
    avgdf=pd.DataFrame(ff["FEEDER TYPE"].value_counts())
    avgdf.sort_values(by="FEEDER TYPE",inplace=True)
    updf.drop("All",inplace=True)
    updf["Average Supply Hours"]=updf["All"]/avgdf["FEEDER TYPE"]
    updf["Supply Hours"]=pd.Timedelta("1 Days")-updf["Average Supply Hours"]
    uppcl_df=(updf.iloc[1:,[2]]["Supply Hours"])
    
    
    
    
    t=df[df["OUTAGE TYPE"]=="SHUTDOWN"].index
    df_ws=df.drop(t)
    ff_o=(ff.pivot_table(index="FEEDER CATEGORY",aggfunc="count"))
    of_os=(df.pivot_table(index="FEEDER CATEGORY",values="TOTAL OUTAGE TIME",aggfunc="sum"))
    of_ows=(df_ws.pivot_table(index="FEEDER CATEGORY",values="TOTAL OUTAGE TIME",aggfunc="sum"))
    of_ocs=(df.pivot_table(index="FEEDER CATEGORY",values="TOTAL OUTAGE TIME",aggfunc="count"))
    of_ocws=(df_ws.pivot_table(index="FEEDER CATEGORY",values="TOTAL OUTAGE TIME",aggfunc="count"))

    odf=pd.DataFrame()

    odf["FEEDER COUNT"]=ff_o["FEEDER TYPE"]
    odf["AVG SUPPLY HOURS INCLUDING SD"]=pd.Timedelta("1 Days")-of_os["TOTAL OUTAGE TIME"]/odf["FEEDER COUNT"]
    odf["OUTAGES COUNT INCLUDING SHUTDOWN"]=of_ocs["TOTAL OUTAGE TIME"]
    odf["AVG SUPPLY HOURS EXCLUDING SD"]=pd.Timedelta("1 Days")-of_ows["TOTAL OUTAGE TIME"]/odf["FEEDER COUNT"]
    odf["OUTAGES COUNT EXCLUDING SHUTDOWN"]=of_ocws["TOTAL OUTAGE TIME"]

    odf["OUTAGES COUNT INCLUDING SHUTDOWN"]["11KV RURAL"]=len(df[(df["OUTAGE TYPE"]!="LOAD SHEDDING") & (df["FEEDER CATEGORY"]=="11KV RURAL")])
    odf["OUTAGES COUNT EXCLUDING SHUTDOWN"]["11KV RURAL"]=len(df[(df["OUTAGE TYPE"]!="LOAD SHEDDING") & (df["FEEDER CATEGORY"]=="11KV RURAL")])
    odf["OUTAGES COUNT INCLUDING SHUTDOWN"]["11KV MIXED"]=len(df[(df["OUTAGE TYPE"]!="LOAD SHEDDING") & (df["FEEDER CATEGORY"]=="11KV MIXED")])
    odf["OUTAGES COUNT EXCLUDING SHUTDOWN"]["11KV MIXED"]=len(df[(df["OUTAGE TYPE"]!="LOAD SHEDDING") & (df["FEEDER CATEGORY"]=="11KV MIXED")])

    odf["AVG SUPPLY HOURS EXCLUDING SD"]["11KV RURAL"]=odf["AVG SUPPLY HOURS INCLUDING SD"]["11KV RURAL"]
    odf["AVG SUPPLY HOURS EXCLUDING SD"]["11KV MIXED"]=odf["AVG SUPPLY HOURS INCLUDING SD"]["11KV MIXED"]

    
    
    







    return render_template("daily_down.html")
@app.route('/download1')
def download_uppcl():

    resp=make_response(uppcl_df.to_csv())
    
    resp.headers["Content-Disposition"]=("attachment;filename=uppcl.csv")
    resp.headers["Content-Type"]="text/csv"

    return (resp)
    

@app.route('/download2')
def download_outage():

    resp=make_response(odf.to_csv())
    
    resp.headers["Content-Disposition"]=("attachment;filename=outage.csv")
    resp.headers["Content-Type"]="text/csv"

    return (resp)

@app.route('/download3')
def download_rural():

    resp=make_response(rural.to_csv())
    
    resp.headers["Content-Disposition"]=("attachment;filename=rural.csv")
    resp.headers["Content-Type"]="text/csv"

    return (resp)
    


@app.route('/monthly_app',methods=['POST'])
def success2():

    global outage_11kv
    global outage_33kv

    import pandas as pd
    import datetime as dt
    import numpy as np
    f=request.files['monthly_export']
    of=pd.read_excel(f)


    ff=pd.read_excel('Feeders.xlsx')

    of.drop(columns=['SUBSTATION','SUBSTATION DESC','OUTAGE ID','STATUS','TOTAL OUTAGE TIME','AGGREGATE TIME',"REASON",'REASON DESCRIPTION','REMARKS','LOCATION','STAND BY FEEDER','SUBTOTAL AGG.',"FEEDER TYPE"],inplace=True)

    of["FEEDER DESC"]=of["FEEDER DESC"].astype(str)
    of.drop(list(of[of["FEEDER DESC"]=="nan"].index),inplace=True)

    ff.drop(columns=["SI. No."],inplace=True)

    off=pd.merge(of,ff,on="FEEDER ID",how="left")


    off.drop(columns=["FEEDER DESC_x"],inplace=True)
    off.rename(columns={"FEEDER DESC_y":"FEEDER DESC"},inplace=True)


    off.dropna(how="any",axis=0,inplace=True)

    df=off[:]

    for i,j in df.iterrows():
    
        if(df["END DATE"][i].month!=12):
            if(df["END DATE"][i].month>df["START DATE"][i].month):
                df["END DATE"][i]=dt.date(df["END DATE"][i].year,df["END DATE"][i].month+1,1)
                df["END TIME"][i]=dt.time(hour=0,minute=0,second=0)
        else:
            if(df["END DATE"][i].month==1):
                df["END DATE"][i]=dt.date(df["END DATE"][i].year+1,1,1)
                df["END TIME"][i]=dt.time(hour=0,minute=0,second=0)
        



    df["START DATE"]=df["START DATE"].astype('str')
    df["END DATE"]=df["END DATE"].astype('str')
    df["START TIME"]=df["START TIME"].astype('str')
    df["END TIME"]=df["END TIME"].astype('str')
    df["temp"]=" "
    df["START TIME"]=df["START DATE"]+df["temp"]+df["START TIME"]
    df["END TIME"]=df["END DATE"]+df["temp"]+df["END TIME"]
    df.drop(columns=["START DATE","END DATE","temp"],inplace=True)
    df["START TIME"]=pd.to_datetime(df["START TIME"])
    df["END TIME"]=pd.to_datetime(df["END TIME"])

    a=df.groupby("FEEDER DESC")
    b=list(df["FEEDER DESC"].unique())

    for i in b:
        x=0
        m=a.get_group(i).sort_values(by="START TIME")
        for j,k in m.iterrows():
            if(x>0):
                if(df["START TIME"][j]<df["END TIME"][m]):
                    df["START TIME"][j]=df["END TIME"][m]
                if(df["END TIME"][j]<df["START TIME"][j]):
                    df["END TIME"][j]=df["START TIME"][j]
            
               
          
                
                
            m=j
            x+=1
        
    df["TOTAL OUTAGE TIME"]=df["END TIME"]-df["START TIME"]

    lst=[]
    for i,j in df.iterrows():
        if df.loc[i]["TOTAL OUTAGE TIME"]==pd.Timedelta("0 days"):
            lst.append(i)
        
    df.drop(lst,inplace=True)

    xf=(df.pivot_table(index="FEEDER ID",columns="OUTAGE TYPE",values="TOTAL OUTAGE TIME",aggfunc="sum",margins=True)).fillna(value=pd.Timedelta("0 days"))
    xff=(df.pivot_table(index="FEEDER ID",columns="OUTAGE TYPE",values="TOTAL OUTAGE TIME",aggfunc="count",margins=True)).fillna(value=0)

    kv11_df=pd.read_excel("Monthly_11 kV_Feeders.xlsx")
    kv33_df=pd.read_excel("Monthly_33 kV_Feeders.xlsx")

    kv11_df.dropna(axis=0,inplace=True)
    kv33_df.dropna(axis=0,inplace=True)


    kv11_df_s=pd.merge(kv11_df,xf,on="FEEDER ID",how="left").fillna(pd.Timedelta("0 days"))
    kv33_df_s=pd.merge(kv33_df,xf,on="FEEDER ID",how="left").fillna(pd.Timedelta("0 days"))

    kv11_df_c=pd.merge(kv11_df,xff,on="FEEDER ID",how="left").fillna(0)
    kv33_df_c=pd.merge(kv33_df,xff,on="FEEDER ID",how="left").fillna(0)

    outage_11kv=pd.DataFrame()
    outage_33kv=pd.DataFrame()

    outage_11kv[["Feeder Name","Substation","Feeder Type","Division","Status"]]=kv11_df_s[["Feeder Name","Substation","Feeder Type","Division","Status"]]
    outage_33kv[["Feeder Name","Substation","Feeder Type","Division"]]=kv33_df_s[["Feeder Name","Substation","Feeder Type","Division"]]

    outage_11kv["NO SUPPLY_count"]=kv11_df_c["NO SUPPLY"]
    outage_11kv["NO SUPPLY_duration"]=kv11_df_s["NO SUPPLY"]

    outage_11kv["LOAD SHEDDING_count"]=kv11_df_c["LOAD SHEDDING"]
    outage_11kv["LOAD SHEDDING_duration"]=kv11_df_s["LOAD SHEDDING"]

    outage_11kv["BREAKDOWN_count"]=kv11_df_c["BREAKDOWN"]
    outage_11kv["BREAKDOWN_duration"]=kv11_df_s["BREAKDOWN"]

    outage_11kv["TRANSIENT FAULT_count"]=kv11_df_c["TRANSIENT FAULT"]
    outage_11kv["TRANSIENT FAULT_duration"]=kv11_df_s["TRANSIENT FAULT"]

    outage_11kv["SHUTDOWN_count"]=kv11_df_c["SHUTDOWN"]
    outage_11kv["SHUTDOWN_duration"]=kv11_df_s["SHUTDOWN"]

    outage_11kv["TOTAL COUNT"]=kv11_df_c["All"]
    outage_11kv["TOTAL duration"]=kv11_df_s["All"]

    outage_33kv["NO SUPPLY_count"]=kv33_df_c["NO SUPPLY"]
    outage_33kv["NO SUPPLY_duration"]=kv33_df_s["NO SUPPLY"]

    outage_33kv["LOAD SHEDDING_count"]=kv33_df_c["LOAD SHEDDING"]
    outage_33kv["LOAD SHEDDING_duration"]=kv33_df_s["LOAD SHEDDING"]

    outage_33kv["BREAKDOWN_count"]=kv33_df_c["BREAKDOWN"]
    outage_33kv["BREAKDOWN_duration"]=kv33_df_s["BREAKDOWN"]

    outage_33kv["TRANSIENT FAULT_count"]=kv33_df_c["TRANSIENT FAULT"]
    outage_33kv["TRANSIENT FAULT_duration"]=kv33_df_s["TRANSIENT FAULT"]

    outage_33kv["SHUTDOWN_count"]=kv33_df_c["SHUTDOWN"]
    outage_33kv["SHUTDOWN_duration"]=kv33_df_s["SHUTDOWN"]

    outage_33kv["TOTAL COUNT"]=kv33_df_c["All"]
    outage_33kv["TOTAL duration"]=kv33_df_s["All"]

    outage_11kv.to_excel("Outage_Report[11 KV].xlsx")
    outage_33kv.to_excel("Outage_Report[33 KV].xlsx")

    return render_template("monthly_down.html")

    

@app.route('/download4')
def monthly_down_11kv():

    resp=make_response(outage_11kv.to_csv())
    
    resp.headers["Content-Disposition"]=("attachment;filename=otage11kv.csv")
    resp.headers["Content-Type"]="text/csv"

    return (resp)
    

@app.route('/download5')
def monthly_down_33kv():
    resp=make_response(outage_33kv.to_csv())
    
    resp.headers["Content-Disposition"]=("attachment;filename=outage33kv.csv")
    resp.headers["Content-Type"]="text/csv"

    return (resp)
    





    



if __name__=='__main__':
    app.run(debug=True)