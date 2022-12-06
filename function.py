import pandas as pd
import pyodbc 

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=PHMANJSANTOS101\LAWSONINSTANCE;'
                      'Database=test_db;'
                      'Trusted_Connection=yes;')
def principal_funds_available():
    df = pd.read_sql_query('SELECT SUM(ScheduledPrincipal)as ScheduledPrincipal,\
                            sum(Curtailments)+ sum(CurtailmentAdjustments) as Curtailments,\
                            sum(Prepayment)as Prepayment,\
                            sum(LiquidationPrincipal) - sum(PrincipalLosses) as NetLiquidationProceeds,\
                            sum(RepurchasePrincipal) as RepurchasePrincipal,\
                            0.00 as SubstitutionPrincipal,\
                            sum(OtherPrincipalAdjustments) as OtherPrincipal,\
                            SUM(ScheduledPrincipal) + (sum(Curtailments) + sum(CurtailmentAdjustments))+\
                            sum(Prepayment) + (sum(LiquidationPrincipal) - sum(PrincipalLosses)) + sum(RepurchasePrincipal)+\
                            sum(OtherPrincipalAdjustments) as TotalPrincipalFundsAvailable\
                            FROM tbl_EnhanceLoanLevel', conn)

    return df.loc[0]
    
def get_scheduled_principal():
    data = pd.read_sql_query('SELECT SUM(ScheduledPrincipal)as ScheduledPrincipal FROM tbl_EnhanceLoanLevel', conn)
    return data

    
def get_curtailments():
    data = pd.read_sql_query('SELECT sum(Curtailments)+ sum(CurtailmentAdjustments) as Curtailments FROM tbl_EnhanceLoanLevel', conn)
    return data
    
def get_prepayment():
    data = pd.read_sql_query('SELECT sum(Prepayment)as Prepayment FROM tbl_EnhanceLoanLevel', conn)
    return data

def get_net_liquidation_proceeds():
    data = pd.read_sql_query('SELECT sum(LiquidationPrincipal) - sum(PrincipalLosses) as NetLiquidationProceeds FROM tbl_EnhanceLoanLevel', conn)
    return data

    
def get_repurchase_principal():
    data = pd.read_sql_query('SELECT sum(RepurchasePrincipal) as RepurchasePrincipal FROM tbl_EnhanceLoanLevel', conn)
    return data
   
def get_other_principal():
    data = pd.read_sql_query('SELECT sum(OtherPrincipalAdjustments) as OtherPrincipal FROM tbl_EnhanceLoanLevel', conn)
    return data