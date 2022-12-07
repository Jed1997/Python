import pandas as pd
import pyodbc 

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=PHMANJSANTOS101\LAWSONINSTANCE;'
                      'Database=test_db;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()

def check_val(DistributionDate,pfa_data):
    sql_d=[]
    diff = []
    i=0
    df = pd.read_sql_query("SELECT SUM(ScheduledPrincipal)as ScheduledPrincipal,\
                            sum(Curtailments)+ sum(CurtailmentAdjustments) as Curtailments,\
                            sum(Prepayment)as Prepayment,\
                            sum(LiquidationPrincipal) - sum(PrincipalLosses) as NetLiquidationProceeds,\
                            SUM(ScheduledPrincipal) + (sum(Curtailments) + sum(CurtailmentAdjustments))+\
                            sum(Prepayment) + (sum(LiquidationPrincipal) - sum(PrincipalLosses))\
                            as TotalPrincipalFundsAvailable\
                            FROM tbl_enhance_loan_level where DistributionDate = "+str(DistributionDate)+";", conn)
    for a in df.loc[0]:
        sql_d.append(a)
        if(float(a)!=float(pfa_data[i])):
            diff.append(float(pfa_data[i])-float(a))
        else:
            diff.append("no difference")
        i = i+1
    data_dict = {}
    data_dict["PDF PFA"] = pfa_data
    data_dict["DB PFA"] = sql_d
    data_dict["Difference"] = diff
    df_res = pd.DataFrame.from_dict(data_dict, orient='index', columns=['ScheduledPrincipal', 'Curtailments', 'Prepayment', 'NetLiquidationProceeds','TotalPrincipalFundsAvailable'])
    #df_res = pd.DataFrame.from_dict(data_dict)
    return df_res

def principal_funds_available(DistributionDate):
    df = pd.read_sql_query("SELECT SUM(ScheduledPrincipal)as ScheduledPrincipal,\
                            sum(Curtailments)+ sum(CurtailmentAdjustments) as Curtailments,\
                            sum(Prepayment)as Prepayment,\
                            sum(LiquidationPrincipal) - sum(PrincipalLosses) as NetLiquidationProceeds,\
                            sum(RepurchasePrincipal) as RepurchasePrincipal,\
                            0.00 as SubstitutionPrincipal,\
                            sum(OtherPrincipalAdjustments) as OtherPrincipal,\
                            SUM(ScheduledPrincipal) + (sum(Curtailments) + sum(CurtailmentAdjustments))+\
                            sum(Prepayment) + (sum(LiquidationPrincipal) - sum(PrincipalLosses)) + sum(RepurchasePrincipal)+\
                            sum(OtherPrincipalAdjustments) as TotalPrincipalFundsAvailable\
                            FROM tbl_enhance_loan_level where DistributionDate = "+str(DistributionDate)+";", conn)

    return df.loc[0]
    
def get_scheduled_principal(DistributionDate):
    data = pd.read_sql_query('SELECT SUM(ScheduledPrincipal)as ScheduledPrincipal FROM tbl_enhance_loan_level where DistributionDate = '+str(DistributionDate)+';', conn)
    return data

    
def get_curtailments(DistributionDate):
    data = pd.read_sql_query('SELECT sum(Curtailments)+ sum(CurtailmentAdjustments) as Curtailments FROM tbl_enhance_loan_level where DistributionDate = '+str(DistributionDate)+';', conn)
    return data
    
def get_prepayment(DistributionDate):
    data = pd.read_sql_query('SELECT sum(Prepayment)as Prepayment FROM tbl_enhance_loan_level where DistributionDate = '+str(DistributionDate)+';', conn)
    return data

def get_net_liquidation_proceeds(DistributionDate):
    data = pd.read_sql_query('SELECT sum(LiquidationPrincipal) - sum(PrincipalLosses) as NetLiquidationProceeds FROM tbl_enhance_loan_level where DistributionDate = '+str(DistributionDate)+';', conn)
    return data

    
def get_repurchase_principal(DistributionDate):
    data = pd.read_sql_query('SELECT sum(RepurchasePrincipal) as RepurchasePrincipal FROM tbl_enhance_loan_level where DistributionDate = '+str(DistributionDate)+';', conn)
    return data
   
def get_other_principal(DistributionDate):
    data = pd.read_sql_query('SELECT sum(OtherPrincipalAdjustments) as OtherPrincipal FROM tbl_enhance_loan_level where DistributionDate = '+str(DistributionDate)+';', conn)
    return data