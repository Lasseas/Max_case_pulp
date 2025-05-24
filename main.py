import numpy as np
import sys
import pandas as pd
import pyomo.environ as pyo
from pyomo.opt import SolverFactory
import time
import os 
import matplotlib.pyplot as plt
import platform
import shutil
#import psutil
from pyomo.environ import *
#from _run_everything import excel_path, instance, year, num_branches_to_firstStage, num_branches_to_secondStage, num_branches_to_thirdStage, num_branches_to_fourthStage, num_branches_to_fifthStage, num_branches_to_sixthStage, num_branches_to_seventhStage, num_branches_to_eighthStage, num_branches_to_ninthStage, num_branches_to_tenthStage
##################################################################
############################### ###############################
##################################################################
##################################################################

import argparse
#from Generate_data_files import run_everything
parser = argparse.ArgumentParser(description="Run model instance")
parser.add_argument("--instance", type=int, required=True, help="Instance number (e.g., 1–5)")
parser.add_argument("--year", type=int, required=True, help="Year (e.g., 2025 or 2050)")
parser.add_argument("--case", type=str, required=True, choices=["wide", "deep", "max"], help="Specify case type")
parser.add_argument("--file", type=str, help = "path name for output file")
args = parser.parse_args()

instance = args.instance
year = args.year
case = args.case
filepath = args.file

excel_path = "NO1_Pulp_Paper_2024_combined historical data_Uten_SatSun.xlsx"
#excel_path = "NO1_Pulp_Paper_2024_combined historical data.xlsx"
#excel_path = "NO1_Aluminum_2024_combined historical data.xlsx"

# Define branch structures for each case type
case_configs = {
    "wide": (2, 30, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    "deep": (2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 0),
    "max":  (2, 5, 5, 5, 5, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0)
}

(
    num_branches_to_firstStage,
    num_branches_to_secondStage,
    num_branches_to_thirdStage,
    num_branches_to_fourthStage,
    num_branches_to_fifthStage,
    num_branches_to_sixthStage,
    num_branches_to_seventhStage,
    num_branches_to_eighthStage,
    num_branches_to_ninthStage,
    num_branches_to_tenthStage,
    num_branches_to_eleventhStage,
    num_branches_to_twelfthStage,
    num_branches_to_thirteenthStage,
    num_branches_to_fourteenthStage,
    num_branches_to_fifteenthStage
) = case_configs[case]


"""
run_everything(
    excel_path,
    instance,
    year,
    num_branches_to_firstStage,
    num_branches_to_secondStage,
    num_branches_to_thirdStage,
    num_branches_to_fourthStage,
    num_branches_to_fifthStage,
    num_branches_to_sixthStage,
    num_branches_to_seventhStage,
    num_branches_to_eighthStage,
    num_branches_to_ninthStage,
    num_branches_to_tenthStage,
    num_branches_to_eleventhStage,
    num_branches_to_twelfthStage,
    num_branches_to_thirteenthStage,
    num_branches_to_fourteenthStage,
    num_branches_to_fifteenthStage
)
"""

def make_tab_file(filename, data_generator, chunk_size=10_000_000):
        """
        Writes a large dataset to a .tab file in chunks using tab as a delimiter.

        Parameters:
            filename (str): Name of the tab-separated file to save (e.g., 'output.tab').
            data_generator (generator): A generator that yields DataFrame chunks.
            chunk_size (int): Number of rows to process per chunk.
        """
        first_chunk = True  # Used to write the header only once

        with open(filename, "w", newline='') as f:
            for df_chunk in data_generator:
                df_chunk.to_csv(f, sep = "\t", index=False, header=first_chunk, lineterminator='\n')
                first_chunk = False

        print(f"{filename} saved successfully!")

cost_activity = {
    "Power_Grid": {1: 0, 2: -1.162, 3: 2000, 4: -2000}, # 1 = Import, 2 = Export, 3 = RT_Import, 4 = RT_Export 
    "ElectricBoiler": {1: 0, 2: 0, 3: 0}, #1 = LT, 2 = MT, 3 = Dummy
    "HP_LT": {1: 0, 2: 0}, #1 = LT, 2 = Dummy
    "HP_MT": {1: 0, 2: 0, 3: 0}, #1 = LT, 2 = MT, 3 = Dummy
    "PV" : {1: 0},
    "P2G": {1: 0},
    "G2P": {1: 0},
    "GasBoiler": {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}, #1 = LT (CH4 mix), 2 = MT (CH4 mix), 3 = LT (CH4), 4 = MT (CH4), 5 = LT (Biogas), 6 = MT (Biogas)
    "GasBoiler_CCS": {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}, #1 = LT (CH4 mix), 2 = MT (CH4 mix), 3 = LT (CH4), 4 = MT (CH4), 5 = LT (Biogas), 6 = MT (Biogas)
    "CHP": {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}, #1 = LT (CH4 mix), 2 = MT (CH4 mix), 3 = LT (CH4), 4 = MT (CH4), 5 = LT (Biogas), 6 = MT (Biogas)
    "CHP_CCS": {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}, #1 = LT (CH4 mix), 2 = MT (CH4 mix), 3 = LT (CH4), 4 = MT (CH4), 5 = LT (Biogas), 6 = MT (Biogas)
    "Biogas_Grid": {1: 64.5, 2: 0}, #1 = Import, 2 = Export
    "CH4_Grid": {1: 39.479, 2: 0}, #1 = Import, 2 = Export
    "CH4_H2_Mixer": {1: 0},
    "DieselReserveGenerator": {1: 148.8},
    "H2_Grid": {1: 150.1502, 2: 0}, #1 = Import, 2 = Export
    "Dummy_Grid": {1: 0} #1 = Export
    }

#####################################################################################
################################ Ble for stor til pushe til git ######################
################################## må genereres i solstorm ##########################
#####################################################################################

def generate_cost_activity(num_nodes, num_timesteps, cost_activity, filename="Par_ActivityCost.tab"):
        def data_generator(chunk_size=10_000_000):
            rows = []
            count = 0
            for node in range(3, num_nodes + 1):
                for tech, mode_costs in cost_activity.items():
                    for mode in mode_costs:
                        for t in range(1, num_timesteps + 1):
                            cost = mode_costs[mode]
                            rows.append({
                                "Node": node,
                                "Time": t,
                                "Technology": tech,
                                "Operational_mode": mode,
                                "Cost": cost
                            })
                            count += 1
                            if count % chunk_size == 0:
                                yield pd.DataFrame(rows)
                                rows = []
            if rows:
                yield pd.DataFrame(rows)

        make_tab_file(filename, data_generator())

generate_cost_activity(num_nodes = 7812, num_timesteps = 24, cost_activity = cost_activity)

#####################################################################################
################################## KONSTANTE SETT ###################################
#####################################################################################
#################### HUSK Å ENDRE DISSE I DE ANDRE FILENE OGSÅ ######################
#####################################################################################

##################################################################################
############################### READING EXCEL FILE ###############################
##################################################################################

# Function to read all sheets in an Excel file and save each as a .tab file in the current directory
def read_all_sheets(excel):
    # Load the Excel file
    input_excel = pd.ExcelFile(excel)
    
    # Loop over each sheet in the workbook
    for sheet in input_excel.sheet_names:
        # Read the current sheet, skipping the first two rows
        input_sheet = pd.read_excel(excel, sheet_name=sheet, skiprows=2)

        # Drop only fully empty rows (optional)
        data_nonempty = input_sheet.dropna(how='all')

        # Replace spaces in column names with underscores
        data_nonempty.columns = data_nonempty.columns.astype(str).str.replace(' ', '_')

        # Fill missing values with an empty string
        data_nonempty = data_nonempty.fillna('')

        # Convert all columns to strings before replacing whitespace characters in values
        data_nonempty = data_nonempty.applymap(lambda x: str(x) if pd.notnull(x) else "")
        
        # Save as a .tab file using only the sheet name as the file namec
        output_filename = f"{sheet}.tab"
        data_nonempty.to_csv(output_filename, header=True, index=False, sep='\t')
        print(f"Saved file: {output_filename}")

# Call the function with your Excel file
read_all_sheets('Input_data_With_dummyGrid_and_RT.xlsx')

####################################################################
######################### MODEL SPECIFICATIONS #####################
####################################################################

model = pyo.AbstractModel()
data = pyo.DataPortal() #Loading the data from a data soruce in a uniform manner (Excel)


"""
SETS 
"""
#Declaring Sets

print("Declaring sets...")

model.Time = pyo.Set(ordered=True) #Set of time periods (hours)
model.Period = pyo.Set(ordered=True) #Set of stages/operational periods
model.LoadShiftingPeriod = pyo.Set(ordered=True) 
#model.LoadShiftingIntervals = pyo.Set(ordered=True)
#model.Time_NO_LoadShift = pyo.Set(dimen = 2, ordered = True) 
#model.TimeLoadShift = pyo.Set(dimen = 3, ordered = True) #Subset of time periods for load shifting in stage s
model.Month = pyo.Set(ordered = True) #Set of months
model.PeriodInMonth = pyo.Set(dimen = 2, ordered = True) #Subset of stages in month m
model.Technology = pyo.Set(ordered = True) #Set of technologies
model.EnergyCarrier = pyo.Set(ordered = True)
model.Mode_of_operation = pyo.Set(ordered = True)
model.TechnologyToEnergyCarrier = pyo.Set(dimen=3, ordered = True)
model.EnergyCarrierToTechnology = pyo.Set(dimen=3, ordered = True)
model.FlexibleLoad = pyo.Set(ordered=True) #Set of flexible loads (batteries)
model.FlexibleLoadForEnergyCarrier = pyo.Set(dimen = 2, ordered = True)
model.Nodes = pyo.Set(ordered=True) #Set of Nodess
model.Nodes_in_stage = pyo.Set(dimen = 2, ordered = True) #Subset of Nodess
model.Nodes_first = pyo.Set(within = model.Nodes) #Subset of Nodess
model.Parent = pyo.Set(ordered=True) #Set of parents
model.Parent_Node = pyo.Set(dimen = 2, ordered = True)


#Reading the Sets, and loading the data
print("Reading sets...")

data.load(filename="Set_of_TimeSteps.tab", format="set", set=model.Time)
data.load(filename="Set_of_Periods.tab", format="set", set=model.Period)
data.load(filename="Set_of_LoadShiftingPeriod.tab", format="set", set=model.LoadShiftingPeriod)
#data.load(filename="Set_of_TimeSteps_NO_LoadShift.tab", format = "set", set=model.Time_NO_LoadShift)
data.load(filename="Set_of_Month.tab", format = "set", set=model.Month)
data.load(filename="Set_of_PeriodsInMonth.tab", format = "set", set=model.PeriodInMonth)
data.load(filename="Set_of_Technology.tab", format = "set", set=model.Technology)
data.load(filename="Set_of_EnergyCarrier.tab", format="set", set=model.EnergyCarrier)
data.load(filename="Set_Mode_of_Operation.tab", format = "set", set = model.Mode_of_operation)
data.load(filename="Subset_TechToEC.tab", format="set", set=model.TechnologyToEnergyCarrier)
data.load(filename="Subset_ECToTech.tab", format="set", set=model.EnergyCarrierToTechnology)
data.load(filename="Set_of_FlexibleLoad.tab", format="set", set=model.FlexibleLoad)
data.load(filename="Set_of_FlexibleLoadForEC.tab", format="set", set=model.FlexibleLoadForEnergyCarrier)
data.load(filename="Set_of_Nodes.tab", format="set", set=model.Nodes)
data.load(filename="Set_of_NodesInStage.tab", format="set", set=model.Nodes_in_stage)
data.load(filename="Subset_NodesFirst.tab", format="set", set=model.Nodes_first)
data.load(filename="Set_of_Parents.tab", format="set", set=model.Parent)
data.load(filename="Set_ParentCoupling.tab", format = "set", set = model.Parent_Node)


"""
PARAMETERS
"""
#Declaring Parameters
print("Declaring parameters...")

#model.Cost_Energy = pyo.Param(model.Nodes, model.Time, model.Technology)  # Cost of using energy source i at time t
model.cost_activity = pyo.Param(model.Nodes, model.Time, model.Technology, model.Mode_of_operation) #Cost of using technology i in mode o at time t
model.Cost_Battery = pyo.Param(model.FlexibleLoad)
#model.Cost_Export = pyo.Param(model.Nodes, model.Time, model.Technology)  # Income from exporting energy to the grid at time t
model.Cost_Expansion_Tec = pyo.Param(model.Technology) #Capacity expansion cost
model.Cost_Expansion_Bat = pyo.Param(model.FlexibleLoad) #Capacity expansion cost
model.Cost_Emission = pyo.Param() #Carbon price
model.Cost_Grid = pyo.Param() #Grid tariff
model.aFRR_Up_Capacity_Price = pyo.Param(model.Nodes, model.Time)  # Capacity Price for aFRR up regulation 
model.aFRR_Dwn_Capacity_Price = pyo.Param(model.Nodes, model.Time)  # Capcaity Price for aFRR down regulation
model.aFRR_Up_Activation_Price = pyo.Param(model.Nodes, model.Time)  # Activation Price for aFRR up regulation 
model.aFRR_Dwn_Activation_Price = pyo.Param(model.Nodes, model.Time)  # Activatioin Price for aFRR down regulation 
model.Spot_Price = pyo.Param(model.Nodes, model.Time)
model.Intraday_Price = pyo.Param(model.Nodes, model.Time)
model.Demand = pyo.Param(model.Nodes, model.Time, model.EnergyCarrier)  # Energy demand 
model.Max_charge_discharge_rate = pyo.Param(model.FlexibleLoad, default = 1) # Maximum symmetric charge and discharge rate
model.Charge_Efficiency = pyo.Param(model.FlexibleLoad)  # Efficiency of charging flexible load b [-]
model.Discharge_Efficiency = pyo.Param(model.FlexibleLoad)  # Efficiency of discharging flexible load b [-]
model.Technology_To_EnergyCarrier_Efficiency = pyo.Param(model.TechnologyToEnergyCarrier) #Efficiency of technology i when supplying fuel e
model.EnergyCarrier_To_Technlogy_Efficiency = pyo.Param(model.EnergyCarrierToTechnology) #Efficiency of technology i when consuming fuel e
model.Max_Storage_Capacity = pyo.Param(model.FlexibleLoad)  # Maximum energy storage capacity of flexible load b [MWh]
model.Self_Discharge = pyo.Param(model.FlexibleLoad)  # Self-discharge rate of flexible load b [%]
model.Initial_SOC = pyo.Param(model.FlexibleLoad)  # Initial state of charge for flexible load b [-]
model.Node_Probability = pyo.Param(model.Nodes)  # Probability of Nodes s [-]
model.Up_Shift_Max = pyo.Param(model.Time)  # Maximum allowable up-shifting in load shifting periods as a percentage of demand [% of demand]
model.Down_Shift_Max = pyo.Param(model.Time)  # Maximum allowable down-shifting in load shifting periods as a percentage of demand [% of demand]
model.Initial_Installed_Capacity = pyo.Param(model.Technology) #Initial installed capacity at site for technology i
model.Ramping_Factor = pyo.Param(model.Technology)
model.Availability_Factor = pyo.Param(model.Nodes, model.Time, model.Technology) #Availability factor for technology delivering to energy carrier 
model.Carbon_Intensity = pyo.Param(model.Technology, model.Mode_of_operation) #Carbon intensity when using technology i in mode o
model.Max_Export = pyo.Param() #Maximum allowable export per year, if no concession is given
model.Activation_Factor_UP_Regulation = pyo.Param(model.Nodes, model.Time) # Activation factor determining the duration of up regulation
model.Activation_Factor_DWN_Regulation = pyo.Param(model.Nodes, model.Time) # Activation factor determining the duration of dwn regulation
model.Activation_Factor_ID_Up = pyo.Param(model.Nodes, model.Time) # Activation factor determining the duration of up regulation
model.Activation_Factor_ID_Dwn = pyo.Param(model.Nodes, model.Time) # Activation factor determining the duration of dwn regulation
model.Available_Excess_Heat = pyo.Param() #Fraction of the total available excess heat at usable temperature level to \\& be used an energy source for the heat pump.
model.Power2Energy_Ratio = pyo.Param(model.FlexibleLoad)
model.Max_CAPEX_tech = pyo.Param(model.Technology)
model.Max_CAPEX_flex = pyo.Param(model.FlexibleLoad)
model.Max_CAPEX = pyo.Param() #Maximum allowable CAPEX
model.Max_Carbon_Emission = pyo.Param() #Maximum allowable carbon emissions per year
model.Last_Period_In_Month = pyo.Param(model.Month) #Last period in month m
model.Cost_LS = pyo.Param(model.EnergyCarrier) #Cost of load shifting for energy carrier e
model.ID_Cap_Buy_volume = pyo.Param(model.Nodes, model.Time) #Volume of ID total bought in the market
model.ID_Cap_Sell_volume = pyo.Param(model.Nodes, model.Time) #Volume of ID total sold in the market
model.Res_Cap_Up_volume = pyo.Param(model.Nodes, model.Time) #Volume of total mFRR up shift in the market
model.Res_Cap_Down_volume = pyo.Param(model.Nodes, model.Time) #Volume of total mFRR down shift in the market

#Reading the Parameters, and loading the data
print("Reading parameters...")

#data.load(filename="Par_EnergyCost.tab", param=model.Cost_Energy, format = "table")
data.load(filename="Par_ActivityCost.tab", param=model.cost_activity, format = "table")
data.load(filename="Par_BatteryCost.tab", param=model.Cost_Battery, format = "table")
#data.load(filename="Par_ExportCost.tab", param=model.Cost_Export, format = "table")
data.load(filename="Par_CostExpansion_Tec.tab", param=model.Cost_Expansion_Tec, format = "table")
data.load(filename="Par_CostExpansion_Bat.tab", param=model.Cost_Expansion_Bat, format = "table")
if instance == 1 and year == 2025:
    data.load(filename="Par_CostEmission_1_2025.tab", param=model.Cost_Emission, format = "table")
elif instance == 1 and year == 2050:
    data.load(filename="Par_CostEmission_1_2050.tab", param=model.Cost_Emission, format = "table")
elif instance == 2 and year == 2025:
    data.load(filename="Par_CostEmission_2_2025.tab", param=model.Cost_Emission, format = "table")
elif instance == 2 and year == 2050:
    data.load(filename="Par_CostEmission_2_2050.tab", param=model.Cost_Emission, format = "table")
elif instance == 3 and year == 2025:
    data.load(filename="Par_CostEmission_3_2025.tab", param=model.Cost_Emission, format = "table")
elif instance == 3 and year == 2050:
    data.load(filename="Par_CostEmission_3_2050.tab", param=model.Cost_Emission, format = "table")
elif instance == 4 and year == 2025:
    data.load(filename="Par_CostEmission_4_2025.tab", param=model.Cost_Emission, format = "table")
elif instance == 4 and year == 2050:
    data.load(filename="Par_CostEmission_4_2050.tab", param=model.Cost_Emission, format = "table")
elif instance == 5 and year == 2025:
    data.load(filename="Par_CostEmission_5_2025.tab", param=model.Cost_Emission, format = "table")
elif instance == 5 and year == 2050:
    data.load(filename="Par_CostEmission_5_2050.tab", param=model.Cost_Emission, format = "table")
else:
    ValueError("Invalid instance or year. Please check the values.")

data.load(filename="Par_CostGridTariff.tab", param=model.Cost_Grid, format = "table")
data.load(filename="Par_aFRR_UP_CAP_price.tab", param=model.aFRR_Up_Capacity_Price, format = "table")
data.load(filename="Par_aFRR_DWN_CAP_price.tab", param=model.aFRR_Dwn_Capacity_Price, format = "table")
data.load(filename="Par_aFRR_UP_ACT_price.tab", param=model.aFRR_Up_Activation_Price, format = "table")
data.load(filename="Par_aFRR_DWN_ACT_price.tab", param=model.aFRR_Dwn_Activation_Price, format = "table")
data.load(filename="Par_SpotPrice.tab", param=model.Spot_Price, format = "table")
data.load(filename="Par_IntradayPrice.tab", param=model.Intraday_Price, format = "table")
data.load(filename="Par_EnergyDemand.tab", param=model.Demand, format = "table")
data.load(filename="Par_MaxChargeDischargeRate.tab", param=model.Max_charge_discharge_rate, format = "table")
data.load(filename="Par_ChargeEfficiency.tab", param=model.Charge_Efficiency, format = "table")
data.load(filename="Par_DischargeEfficiency.tab", param=model.Discharge_Efficiency, format = "table")
data.load(filename="Par_TechToEC_Efficiency.tab", param=model.Technology_To_EnergyCarrier_Efficiency, format = "table")
data.load(filename="Par_ECToTech_Efficiency.tab", param=model.EnergyCarrier_To_Technlogy_Efficiency, format = "table")
data.load(filename="Par_MaxStorageCapacity.tab", param=model.Max_Storage_Capacity, format = "table")
data.load(filename="Par_SelfDischarge.tab", param=model.Self_Discharge, format = "table")
data.load(filename="Par_InitialSoC.tab", param=model.Initial_SOC, format = "table")
data.load(filename="Par_NodesProbability.tab", param=model.Node_Probability, format = "table")
#data.load(filename="Par_MaxCableCapacity.tab", param=model.Max_Cable_Capacity, format = "table")
data.load(filename="Par_MaxUpShift.tab", param=model.Up_Shift_Max, format = "table")
data.load(filename="Par_MaxDwnShift.tab", param=model.Down_Shift_Max, format = "table")
data.load(filename="Par_InitialCapacityInstalled.tab", param=model.Initial_Installed_Capacity, format = "table")
data.load(filename="Par_AvailabilityFactor.tab", param=model.Availability_Factor, format = "table")
data.load(filename="Par_CarbonIntensity.tab", param=model.Carbon_Intensity, format = "table")
data.load(filename="Par_MaxExport.tab", param=model.Max_Export, format = "table")
data.load(filename="Par_ActivationFactor_Up_Reg.tab", param=model.Activation_Factor_UP_Regulation, format = "table")
data.load(filename="Par_ActivationFactor_Dwn_Reg.tab", param=model.Activation_Factor_DWN_Regulation, format = "table")
data.load(filename="Par_ActivationFactor_ID_Up_Reg.tab", param=model.Activation_Factor_ID_Up, format = "table")
data.load(filename="Par_ActivationFactor_ID_Dwn_Reg.tab", param=model.Activation_Factor_ID_Dwn, format = "table")
data.load(filename="Par_AvailableExcessHeat.tab", param=model.Available_Excess_Heat, format = "table")
data.load(filename="Par_Power2Energy_ratio.tab", param=model.Power2Energy_Ratio, format = "table")
data.load(filename="Par_Ramping_factor.tab", param=model.Ramping_Factor, format = "table")
data.load(filename="Par_Max_Capex_tec.tab", param=model.Max_CAPEX_tech, format = "table")
data.load(filename="Par_Max_Capex_bat.tab", param=model.Max_CAPEX_flex, format = "table")
data.load(filename="Par_Max_CAPEX.tab", param=model.Max_CAPEX, format = "table")
data.load(filename="Par_Max_Carbon_Emission.tab", param=model.Max_Carbon_Emission, format = "table")
data.load(filename="Par_LastPeriodInMonth.tab", param=model.Last_Period_In_Month, format = "table")
data.load(filename="Par_Cost_LS.tab", param=model.Cost_LS, format = "table")
data.load(filename="Par_ID_Capacity_Buy_Volume.tab", param=model.ID_Cap_Buy_volume, format = "table")
data.load(filename="Par_ID_Capacity_Sell_Volume.tab", param=model.ID_Cap_Sell_volume, format = "table")
data.load(filename="Par_Res_CapacityUpVolume.tab", param=model.Res_Cap_Up_volume, format = "table")
data.load(filename="Par_Res_CapacityDownVolume.tab", param=model.Res_Cap_Down_volume, format = "table")


"""
VARIABLES
"""
#Declaring Variables
model.x_UP = pyo.Var(model.Nodes, model.Time, domain= pyo.NonNegativeReals)#, bounds = (0,0))
model.x_DWN = pyo.Var(model.Nodes, model.Time, domain= pyo.NonNegativeReals)#, bounds = (0,0))
model.x_DA_buy = pyo.Var(model.Nodes, model.Time, domain= pyo.NonNegativeReals)
model.x_DA_sell = pyo.Var(model.Nodes, model.Time, domain= pyo.NonNegativeReals)
model.x_ID_buy = pyo.Var(model.Nodes, model.Time, domain= pyo.NonNegativeReals)
model.x_ID_sell = pyo.Var(model.Nodes, model.Time, domain= pyo.NonNegativeReals)
model.y_out = pyo.Var(model.Nodes, model.Time, model.TechnologyToEnergyCarrier, domain = pyo.NonNegativeReals)
model.y_in = pyo.Var(model.Nodes, model.Time, model.EnergyCarrierToTechnology, domain = pyo.NonNegativeReals)
model.y_activity = pyo.Var(model.Nodes, model.Time, model.Technology, model.Mode_of_operation, domain = pyo.NonNegativeReals)
model.q_charge = pyo.Var(model.Nodes, model.Time, model.FlexibleLoad, domain= pyo.NonNegativeReals)
model.q_discharge = pyo.Var(model.Nodes, model.Time, model.FlexibleLoad, domain= pyo.NonNegativeReals)
model.q_SoC = pyo.Var(model.Nodes, model.Time, model.FlexibleLoad, domain= pyo.NonNegativeReals)
model.v_new_tech = pyo.Var(model.Technology, domain = pyo.NonNegativeReals, bounds = (0,0)) 
model.v_new_bat = pyo.Var(model.FlexibleLoad, domain = pyo.NonNegativeReals, bounds = (0,0))
model.y_max = pyo.Var(model.Nodes, model.Month, domain = pyo.NonNegativeReals)
model.d_flex = pyo.Var(model.Nodes, model.Time, model.EnergyCarrier, domain = pyo.NonNegativeReals)
model.Up_Shift = pyo.Var(model.Nodes, model.Time, model.EnergyCarrier, domain = pyo.NonNegativeReals)
model.Dwn_Shift = pyo.Var(model.Nodes, model.Time, model.EnergyCarrier, domain = pyo.NonNegativeReals)
model.aggregated_Up_Shift = pyo.Var(model.Nodes, model.EnergyCarrier, domain = pyo.NonNegativeReals)
model.aggregated_Dwn_Shift = pyo.Var(model.Nodes, model.EnergyCarrier, domain = pyo.NonNegativeReals)
model.Not_Supplied_Energy = pyo.Var(model.Nodes, model.Time, model.EnergyCarrier, domain = pyo.NonNegativeReals)
model.I_loadShedding = pyo.Var()
model.I_inv = pyo.Var()
model.I_GT = pyo.Var()
model.I_cap_bid = pyo.Var(model.Time)
model.I_activation = pyo.Var(model.Nodes, model.Time)
model.I_DA = pyo.Var(model.Nodes, model.Time)
model.I_ID = pyo.Var(model.Nodes, model.Time)
model.I_OPEX = pyo.Var(model.Nodes, model.Time)


"""
OBJECTIVE
""" 

#OBJECTIVE SHORT FORM
def objective(model):
    obj_expr = model.I_inv + model.I_GT + sum(
        model.I_cap_bid[t] + sum(sum(
            model.Node_Probability[n] * (
                model.I_activation[n, t] + model.I_DA[n, t] + model.I_ID[n, t] + model.I_OPEX[n, t]
            ) for (n, stage) in model.Nodes_in_stage if stage == s
        ) for s in model.Period    
    ) for t in model.Time)

    return obj_expr

model.Objective = pyo.Objective(rule=objective, sense=pyo.minimize)

"""
CONSTRAINTS
"""  

###########################################
############## COST BALANCES ##############
###########################################
def cost_investment(model):
    return model.I_inv == (sum(
        model.Cost_Expansion_Tec[i] * model.v_new_tech[i] for i in model.Technology
    ) + sum(
        model.Cost_Expansion_Bat[b] * model.v_new_bat[b] for b in model.FlexibleLoad
    ))
model.InvestmentCost = pyo.Constraint(rule=cost_investment)

def cost_grid_tariff(model):
    return model.I_GT == sum(sum(model.Node_Probability[n] * model.Cost_Grid * model.y_max[n, m] for (n,s) in model.Nodes_in_stage if s == model.Last_Period_In_Month[m]) for m in model.Month)
model.GridTariffCost = pyo.Constraint(rule=cost_grid_tariff)

def cost_capacity_bid(model, t):
    nodes_in_last_stage = {n for (n, stage) in model.Nodes_in_stage if stage == model.Period.last()}
    
    return model.I_cap_bid[t] == sum(
        model.Node_Probability[n] * (
            - (model.aFRR_Up_Capacity_Price[n, t] * model.x_UP[n, t] +
               model.aFRR_Dwn_Capacity_Price[n, t] * model.x_DWN[n, t])
        ) for n in model.Nodes if n not in nodes_in_last_stage
    )
model.CapacityBidCost = pyo.Constraint(model.Time, rule=cost_capacity_bid)

def cost_activation(model, n, p, t, s):
    if (n, s) in model.Nodes_in_stage:
        return model.I_activation[n, t] == (- model.Activation_Factor_UP_Regulation[n, t] * model.aFRR_Up_Activation_Price[n, t] * model.x_UP[p, t]
                + model.Activation_Factor_DWN_Regulation[n, t] * model.aFRR_Dwn_Activation_Price[n, t] * model.x_DWN[p, t])
    else:
        return pyo.Constraint.Skip
model.ActivationCost = pyo.Constraint(model.Parent_Node, model.Time, model.Period, rule=cost_activation)

def cost_DA(model, n, p, t, s):
    if (n,s) in model.Nodes_in_stage:
        return model.I_DA[n, t] == model.Spot_Price[n, t] * (model.x_DA_buy[p, t] - model.x_DA_sell[p, t])
    else:
        return pyo.Constraint.Skip
model.DACost = pyo.Constraint(model.Parent_Node, model.Time, model.Period, rule=cost_DA) 

def cost_ID(model, n, p, t, s):
    if (n,s) in model.Nodes_in_stage:
        return model.I_ID[n, t] == model.Intraday_Price[n, t] * (
                model.Activation_Factor_ID_Up[n, t] * model.x_ID_buy[p, t] 
                - model.Activation_Factor_ID_Dwn[n, t] * model.x_ID_sell[p, t]
            )
    else:
        return pyo.Constraint.Skip
model.IDCost = pyo.Constraint(model.Parent_Node, model.Time, model.Period, rule=cost_ID)    
"""
def cost_opex(model, n, s, t):
    return model.I_OPEX[n, t] == (sum(
                model.y_out[n, t, i, e, o] * (model.Cost_Energy[n, t, i] 
                + model.Carbon_Intensity[i, o] * model.Cost_Emission)
                for (i, e, o) in model.TechnologyToEnergyCarrier 
            ) 
            - sum(model.Cost_Export[n, t, i] * model.y_in[n, t, i, e, o] for (i, e, o) in model.EnergyCarrierToTechnology)
            + sum(model.Cost_Battery[b] * model.q_discharge[n, t, b] for b in model.FlexibleLoad)
            #Legge til Load shift cost?
    )
model.OPEXCost = pyo.Constraint(model.Nodes_in_stage, model.Time, rule=cost_opex)
"""
def cost_opex(model, n, s, t):
    return model.I_OPEX[n, t] == (sum(
                model.y_activity[n, t, i, o] * (model.cost_activity[n, t, i, o] 
                + model.Carbon_Intensity[i, o] * model.Cost_Emission)
                for (i, e, o) in model.TechnologyToEnergyCarrier 
            ) 
            - sum(model.cost_activity[n, t, i, o] * model.y_activity[n, t, i, o] for (i, e, o) in model.EnergyCarrierToTechnology)
            + sum(model.Cost_Battery[b] * model.q_discharge[n, t, b] for b in model.FlexibleLoad)
            + sum(model.Cost_LS[e]*model.Dwn_Shift[n, t, e] + 10_000 * model.Not_Supplied_Energy[n, t, e] for e in model.EnergyCarrier)
    )
model.OPEXCost = pyo.Constraint(model.Nodes_in_stage, model.Time, rule=cost_opex)

def cost_load_shedding(model):
    return model.I_loadShedding == sum(sum(sum(model.Node_Probability[n] * 10_000 * model.Not_Supplied_Energy[n, t, e] for (n,s) in model.Nodes_in_stage if s == model.Period) for t in model.Time) for e in model.EnergyCarrier)
model.CostLoadShedding = pyo.Constraint(rule=cost_load_shedding)


###########################################
############## ENERGY BALANCE #############
###########################################

def energy_balance(model, n, s, t, e):
    return (
        model.d_flex[n, t, e]
        == sum(sum(model.y_out[n, t, i, e, o] for i in model.Technology if (i,e,o) in model.TechnologyToEnergyCarrier)
        - sum(model.y_in[n, t, i, e, o] for i in model.Technology if(i,e,o) in model.EnergyCarrierToTechnology) for o in model.Mode_of_operation)
        - sum(
            model.Charge_Efficiency[b] * model.q_charge[n, t, b] - model.q_discharge[n, t, b]
            for b in model.FlexibleLoad if (b,e) in model.FlexibleLoadForEnergyCarrier
        )
    )
model.EnergyBalance = pyo.Constraint(model.Nodes_in_stage, model.Time, model.EnergyCarrier, rule=energy_balance)

def Defining_flexible_demand(model, n, s, t, e):
    return model.d_flex[n, t, e] == model.Demand[n, t, e] + model.Up_Shift[n, t, e] - model.Dwn_Shift[n, t, e] - model.Not_Supplied_Energy[n, t, e]
model.DefiningFlexibleDemand = pyo.Constraint(model.Nodes_in_stage, model.Time, model.EnergyCarrier, rule = Defining_flexible_demand)

#####################################################################################
########################### MARKET BALANCE DA/ID/RT #################################
#####################################################################################

def market_balance_import(model, n, p, t, s, i, e, o):
    if (i, e, o) == ("Power_Grid", "Electricity", 1) and (n,s) in model.Nodes_in_stage:
        return (model.y_out[n, t, i, e, o] == model.x_DA_buy[p, t] + model.Activation_Factor_ID_Up[n,t]*model.x_ID_buy[p, t] + model.Activation_Factor_DWN_Regulation[n, t] * model.x_DWN[p, t])
    else:
        return pyo.Constraint.Skip      
model.MarketBalanceImport = pyo.Constraint(model.Parent_Node, model.Time, model.Period, model.TechnologyToEnergyCarrier, rule = market_balance_import)

def market_balance_export(model, n, p, t, s, i, e, o):
    if (i, e, o) == ("Power_Grid", "Electricity", 2) and (n,s) in model.Nodes_in_stage:
        return (model.y_in[n, t, i, e, o] == model.x_DA_sell[p, t] + model.Activation_Factor_ID_Dwn[n,t]*model.x_ID_sell[p, t] + model.Activation_Factor_UP_Regulation[n, t] * model.x_UP[p, t])
    else:
        return pyo.Constraint.Skip      
model.MarketBalanceExport = pyo.Constraint(model.Parent_Node, model.Time, model.Period, model.EnergyCarrierToTechnology, rule = market_balance_export)

def Max_ID_Buy_Adjustment(model, n, t):
    nodes_in_last_stage = {n for (n, stage) in model.Nodes_in_stage if stage == model.Period.last()}
    if n not in nodes_in_last_stage:
        return (model.x_ID_buy[n, t] <= 0.2*model.ID_Cap_Buy_volume[n, t])
    else:
        return pyo.Constraint.Skip
model.MaxIDBuyAdjustment = pyo.Constraint(model.Nodes, model.Time, rule = Max_ID_Buy_Adjustment)

def Max_ID_Sell_Adjustment(model, n, t):
    nodes_in_last_stage = {n for (n, stage) in model.Nodes_in_stage if stage == model.Period.last()}
    if n not in nodes_in_last_stage:
        return (model.x_ID_sell[n, t] <= 0.2*model.ID_Cap_Sell_volume[n, t])
    else:
        return pyo.Constraint.Skip
model.MaxIDSellAdjustment = pyo.Constraint(model.Nodes, model.Time, rule = Max_ID_Sell_Adjustment)

#####################################################################################
########################### CONVERSION BALANCE ######################################
#####################################################################################

def conversion_balance_out(model, n, s, t, i, e, o):   
    return (model.y_out[n, t, i, e, o] == model.y_activity[n, t, i, o] * model.Technology_To_EnergyCarrier_Efficiency[i, e, o])     
model.ConversionBalanceOut = pyo.Constraint(model.Nodes_in_stage, model.Time, model.TechnologyToEnergyCarrier, rule = conversion_balance_out)

def conversion_balance_in(model, n, s, t, i, e, o):
    return (model.y_in[n, t, i, e, o] == model.y_activity[n, t, i, o] * model.EnergyCarrier_To_Technlogy_Efficiency[i, e, o])           
model.ConversionBalanceIn = pyo.Constraint(model.Nodes_in_stage, model.Time, model.EnergyCarrierToTechnology, rule = conversion_balance_in)

#####################################################################################
########################### TECHNOLOGY RAMPING CONSTRAINTS ##########################
#####################################################################################
"""
def Ramping_Technology(model, n, p, t, s, i, e, o):
    if (n,s) in model.Nodes_in_stage:
        if t == model.Time.first() and s == model.Period.first(): #Første tidssteg i første stage  
            return (model.y_out[n, t, i, e, o] <= model.Ramping_Factor[i] * (model.Initial_Installed_Capacity[i] + model.v_new_tech[i]))
        
        elif t == model.Time.first() and s > model.Period.first():
            return (model.y_out[n, t, i, e, o] - model.y_out[p, model.Time.last(), i, e, o] <= model.Ramping_Factor[i] * (model.Initial_Installed_Capacity[i] + model.v_new_tech[i]))

        else:
            return (model.y_out[n, t, i, e, o] - model.y_out[n, t-1, i, e, o] <= model.Ramping_Factor[i] * (model.Initial_Installed_Capacity[i] + model.v_new_tech[i]))
    else:
        return pyo.Constraint.Skip
model.RampingTechnology = pyo.Constraint(model.Parent_Node, model.Time, model.Period, model.TechnologyToEnergyCarrier, rule = Ramping_Technology)

"""

#####################################################################################
############## HEAT PUMP LIMITATION - MÅ ENDRES I HENHOLD TIL INPUTDATA #############
#####################################################################################
"""
def heat_pump_input_limitation_LT(model, n, s, t):
    return (
        model.y_out[n, t, 'HP_LT', 'LT', 1] - model.y_in[n, t, 'HP_LT', 'Electricity', 1]
        <= model.Available_Excess_Heat * (model.d_flex[n, t, 'LT'])# + model.Demand[s, t, 'HT'])
    )
model.HeatPumpInputLimitationLT = pyo.Constraint(model.Nodes_in_stage, model.Time, rule=heat_pump_input_limitation_LT)

def heat_pump_input_limitation_MT(model, n, s, t):
    return (
        model.y_out[n, t, 'HP_MT', 'MT', 1] - model.y_in[n, t, 'HP_MT', 'Electricity', 1]
        <= model.Available_Excess_Heat * (model.d_flex[n, t, 'MT'])# + model.Demand[s, t, 'HT'])
    )
model.HeatPumpInputLimitationMT = pyo.Constraint(model.Nodes_in_stage, model.Time, rule=heat_pump_input_limitation_MT)
"""

def heat_pump_input_limitation(model, n, s, t):
    return (
        model.y_out[n, t, 'HP_MT', 'MT', 1] - model.y_in[n, t, 'HP_MT', 'Electricity', 1] 
        + model.y_out[n, t, 'HP_MT', 'LT', 2] - model.y_in[n, t, 'HP_MT', 'Electricity', 2] 
        + model.y_out[n, t, 'HP_LT', 'LT', 1] - model.y_in[n, t, 'HP_LT', 'Electricity', 1]
        <= model.Available_Excess_Heat * (model.d_flex[n, t, 'LT'] + model.d_flex[n, t, 'MT'])
    )
model.HeatPumpInputLimitation = pyo.Constraint(model.Nodes_in_stage, model.Time, rule=heat_pump_input_limitation)


######################################################
############## LOAD SHIFTING CONSTRAINTS #############
######################################################

def aggregated_up_shift(model, n, p, e):
    return model.aggregated_Up_Shift[n, e] == model.aggregated_Up_Shift[p, e] + sum(model.Up_Shift[n, t, e] for t in model.Time)
model.AggregatedUpShift = pyo.Constraint(model.Parent_Node, model.EnergyCarrier, rule=aggregated_up_shift)

def aggregated_dwn_shift(model, n, p, e):
    return model.aggregated_Dwn_Shift[n, e] == model.aggregated_Dwn_Shift[p, e] + sum(model.Dwn_Shift[n, t, e] for t in model.Time)
model.AggregatedDwnShift = pyo.Constraint(model.Parent_Node, model.EnergyCarrier, rule=aggregated_dwn_shift)

def balancing_aggregated_shifted_load(model, n, s, e):
    if s in model.LoadShiftingPeriod:
        return model.aggregated_Up_Shift[n, e] == model.aggregated_Dwn_Shift[n, e]
    else:
        return pyo.Constraint.Skip
model.BalancingAggregatedShiftedLoad = pyo.Constraint(model.Nodes_in_stage, model.EnergyCarrier, rule=balancing_aggregated_shifted_load)

def initialize_aggregated_up_shift(model, n, e):
    return model.aggregated_Up_Shift[n, e] == 0
model.InitializeAggregatedUpShift = pyo.Constraint(model.Nodes_first, model.EnergyCarrier, rule=initialize_aggregated_up_shift)

def initialize_aggregated_dwn_shift(model, n, e):
    return model.aggregated_Dwn_Shift[n, e] == 0
model.InitializeAggregatedDwnShift = pyo.Constraint(model.Nodes_first, model.EnergyCarrier, rule=initialize_aggregated_dwn_shift)

"""
def No_Up_Shift_outside_window(model, n, s, t, e):
    if (t,s) in model.Time_NO_LoadShift:
        return model.Up_Shift[n, t, e] == 0
    else:
        return pyo.Constraint.Skip
model.NoUpShiftOutsideWindow = pyo.Constraint(model.Nodes_in_stage, model.Time, model.EnergyCarrier, rule=No_Up_Shift_outside_window)

def No_Dwn_Shift_outside_window(model, n, s, t, e):
    if (t,s) in model.Time_NO_LoadShift:
        return model.Dwn_Shift[n, t, e] == 0
    else:
        return pyo.Constraint.Skip
model.NoDwnShiftOutsideWindow = pyo.Constraint(model.Nodes_in_stage, model.Time, model.EnergyCarrier, rule=No_Dwn_Shift_outside_window)
"""

###########################################################
############## MAX ALLOWABLE UP/DOWN SHIFT ################
###########################################################

def max_up_shift(model, n, s, t, e):
    return model.Up_Shift[n, t, e] <= model.Up_Shift_Max[t] * model.Demand[n, t, e]    
model.MaxUpShift = pyo.Constraint(model.Nodes_in_stage, model.Time, model.EnergyCarrier, rule=max_up_shift)

def max_dwn_shift(model, n, s, t, e):
    return model.Dwn_Shift[n, t, e] <= model.Down_Shift_Max[t] * model.Demand[n, t, e]
model.MaxDwnShift = pyo.Constraint(model.Nodes_in_stage, model.Time, model.EnergyCarrier, rule=max_dwn_shift)

"""
def Max_total_up_dwn_load_shift(model, n, s, t, e):
    return model.Up_Shift[n,t,e] + model.Dwn_Shift[n,t,e] <= model.Up_Shift_Max * model.Demand[n, t, e] 
model.MaxTotalUpDwnLoadShift = pyo.Constraint(model.Nodes_in_stage, model.Time, model.EnergyCarrier, rule=Max_total_up_dwn_load_shift)
"""

########################################################################
############## RESERVE MARKET PARTICIPATION LIMITS #####################
########################################################################
"""
def reserve_down_limit(model, n, p, t, s, e):
    if e == "Electricity" and (n,s) in model.Nodes_in_stage:  # Ensure e = EL
        return model.x_DWN[p, t] <= (
            model.Up_Shift_Max[t] * model.Demand[n, t, e]
            + sum(
                model.Max_charge_discharge_rate[b] + model.Power2Energy_Ratio[b] * model.v_new_bat[b]
                for b in model.FlexibleLoad if (b, e) in model.FlexibleLoadForEnergyCarrier
            )
        )
    else:
        return pyo.Constraint.Skip
model.ReserveDownLimit = pyo.Constraint(model.Parent_Node, model.Time, model.Period, model.EnergyCarrier, rule=reserve_down_limit)

def reserve_up_limit(model, n, p, t, s, e):
    if e == "Electricity" and (n,s) in model.Nodes_in_stage:  # Ensure e = EL
        return model.x_UP[p, t] <= (
            model.Down_Shift_Max[t] * model.Demand[n, t, e]
            + sum(
                model.Max_charge_discharge_rate[b] + model.Power2Energy_Ratio[b] * model.v_new_bat[b]
                for b in model.FlexibleLoad if (b, e) in model.FlexibleLoadForEnergyCarrier
            )
        )
    else:
        return pyo.Constraint.Skip
model.ReserveUpLimit = pyo.Constraint(model.Parent_Node, model.Time, model.Period, model.EnergyCarrier, rule=reserve_up_limit)
"""
########################################################################
############## UPPER-UPPER BOUND CAPACITY MARKET BIDS ##################
########################################################################

def max_capacity_up_bid(model, n, t):
    return model.x_UP[n,t] <= min(50, 0.2*model.Res_Cap_Up_volume[n,t])
model.MaxCapacityUpBid = pyo.Constraint(model.Nodes, model.Time, rule=max_capacity_up_bid)

def max_capacity_down_bid(model, n, t):
    return model.x_DWN[n,t] <= min(50, 0.2*model.Res_Cap_Down_volume[n,t])
model.MaxCapacityDownBid = pyo.Constraint(model.Nodes, model.Time, rule=max_capacity_down_bid)
"""
def maximum_market_down_reserve_limit(model, n, t):
    return model.x_DWN[n,t] <= 0.2*model.Res_Cap_Down_volume[n,t] #Limiting 
model.MaxMarketDownReserveLimit = pyo.Constraint(model.Nodes, model.Time, rule=maximum_market_down_reserve_limit)

def maximum_market_up_reserve_limit(model, n, t):
    return model.x_UP[n,t] <= 0.2*model.Res_Cap_Up_volume[n,t]
model.MaxMarketUpReserveLimit = pyo.Constraint(model.Nodes, model.Time, rule=maximum_market_up_reserve_limit)
"""
########################################################################
############## FLEXIBLE ASSET CONSTRAINTS/STORAGE DYNAMICS #############
########################################################################
def flexible_asset_charge_discharge_limit(model, n, s, t, b, e):
    return (
        model.q_charge[n, t, b] 
        + model.q_discharge[n, t, b] / model.Discharge_Efficiency[b] 
        <= model.Max_charge_discharge_rate[b] + model.Power2Energy_Ratio[b] * model.v_new_bat[b]
    )
model.FlexibleAssetChargeDischargeLimit = pyo.Constraint(model.Nodes_in_stage, model.Time, model.FlexibleLoadForEnergyCarrier, rule=flexible_asset_charge_discharge_limit)

def state_of_charge(model, n, p, t, s, b, e):
    if (n,s) in model.Nodes_in_stage:
        if t == model.Time.first() and s == model.Period.first() :  # Initialisation of flexible assets
            return (
                model.q_SoC[n, t, b]
                == model.Initial_SOC[b] * (model.Max_Storage_Capacity[b] + model.v_new_bat[b]) * (1 - model.Self_Discharge[b])
                + model.q_charge[n, t, b]
                - model.q_discharge[n, t, b] / model.Discharge_Efficiency[b]
            )
        elif t == model.Time.first() and s > model.Period.first():  #Overgangen mellom stages
            return (
                model.q_SoC[n, t, b]
                == model.q_SoC[p, model.Time.last(), b] * (1 - model.Self_Discharge[b])
                + model.q_charge[n, t, b]
                - model.q_discharge[n, t, b] / model.Discharge_Efficiency[b]
            )
        else:        
            return (
                model.q_SoC[n, t, b]
                == model.q_SoC[n, t-1, b] * (1 - model.Self_Discharge[b])
                + model.q_charge[n, t, b]
                - model.q_discharge[n, t, b] / model.Discharge_Efficiency[b]
            )
    else:
        return pyo.Constraint.Skip
model.StateOfCharge = pyo.Constraint(model.Parent_Node, model.Time, model.Period, model.FlexibleLoadForEnergyCarrier, rule=state_of_charge)

def end_of_horizon_SoC(model, n, s, t, b, e):
    if t == model.Time.last() and s == model.Period.last():
        return model.q_SoC[n, t, b] == model.Initial_SOC[b] * (model.Max_Storage_Capacity[b] + model.v_new_bat[b])
    else:
        return pyo.Constraint.Skip
model.EndOfHorizonSoC = pyo.Constraint(model.Nodes_in_stage, model.Time, model.FlexibleLoadForEnergyCarrier, rule = end_of_horizon_SoC)

def flexible_asset_energy_limit(model, n, s, t, b, e):
    return model.q_SoC[n, t, b] <= model.Max_Storage_Capacity[b] + model.v_new_bat[b]
model.FlexibleAssetEnergyLimits = pyo.Constraint(model.Nodes_in_stage, model.Time, model.FlexibleLoadForEnergyCarrier, rule=flexible_asset_energy_limit)

####################################################
############## AVAILABILITY CONSTRAINT #############
####################################################

def supply_limitation(model, n, s, t, i):
    return (sum(model.y_out[n, t, i, e, o] for e,o in model.EnergyCarrier * model.Mode_of_operation if (i,e,o) in model.TechnologyToEnergyCarrier)  
                <= model.Availability_Factor[n, t, i] * (model.Initial_Installed_Capacity[i] + model.v_new_tech[i]))
model.SupplyLimitation = pyo.Constraint(model.Nodes_in_stage, model.Time, model.Technology, rule=supply_limitation)

##############################################################
############## EXPORT LIMITATION AND GRID TARIFF #############
##############################################################
"""
def export_limitation(model, n, s, t, i, e, o):
    if (i, e, o) == ('Power_Grid', 'Electricity', 2):
        return model.y_in[n, t, i, e, o] <= model.Max_Export
    else:
        return pyo.Constraint.Skip
model.ExportLimitation = pyo.Constraint(model.Nodes_in_stage, model.Time, model.EnergyCarrierToTechnology, rule=export_limitation)
"""
def peak_load(model, n, s, t, m, i, e, o):
    if i == 'Power_Grid' and e == 'Electricity' and (m,s) in model.PeriodInMonth:
        return sum(model.y_out[n, t, i, e, o] for o in model.Mode_of_operation if (i,e,o) in model.TechnologyToEnergyCarrier) <= model.y_max[n, m]
    else:
        return pyo.Constraint.Skip
model.PeakLoad = pyo.Constraint(model.Nodes_in_stage, model.Time, model.Month, model.TechnologyToEnergyCarrier, rule=peak_load)

def Node_greater_than_parent(model, n, p, s, m):
    """
    if (n,s) in model.Nodes_in_stage and (m,s) in model.PeriodInMonth:
        return model.y_max[p, m] <= model.y_max[n, m]
    else:
        return pyo.Constraint.Skip
    """
    # n i stage s og måned m
    if (n, s) in model.Nodes_in_stage and (m, s) in model.PeriodInMonth:
        # Finn alle s_p der p er i den samme måneden
        for s_p in model.Period:
            if (p, s_p) in model.Nodes_in_stage and (m, s_p) in model.PeriodInMonth:
                return model.y_max[p, m] <= model.y_max[n, m]
    return pyo.Constraint.Skip
model.NodeGreaterThanParent = pyo.Constraint(model.Parent_Node, model.Period, model.Month, rule = Node_greater_than_parent)

##############################################################
##################### INVESTMENT LIMITATIONS #################
##############################################################
"""
def CAPEX_technology_limitations(model, i):
    return (model.Cost_Expansion_Tec[i] * model.v_new_tech[i] <= model.Max_CAPEX_tech[i])
model.CAPEXTechnologyLim = pyo.Constraint(model.Technology, rule=CAPEX_technology_limitations)

def CAPEX_flexibleLoad_limitations(model, b):
    return (model.Cost_Expansion_Bat[b] * model.v_new_bat[b] <= model.Max_CAPEX_flex[b])
model.CAPEXFlexibleLoadLim = pyo.Constraint(model.FlexibleLoad, rule=CAPEX_flexibleLoad_limitations)
"""
def CAPEX_limitations(model):
    return model.I_inv <= model.Max_CAPEX
model.CAPEXLim = pyo.Constraint(rule=CAPEX_limitations)

##############################################################
##################### CARBON EMISSION LIMIT ##################
##############################################################
"""
def Carbon_Emission_Limit(model, n): #Kan løses med aggregert variabel og parent-nodes
    total_emission = sum(
        model.y_activity[n, t, i, o] * model.Carbon_Intensity[i, o]
        for t in model.Time
        for (i,e,o) in model.TechnologyToEnergyCarrier
    )
    return total_emission <= model.Max_Carbon_Emission
model.CarbonEmissionLimit = pyo.Constraint(model.Nodes_in_stage, rule=Carbon_Emission_Limit)
"""
"""
def Carbon_Emission_Limit(model, n, s): 
    return sum(sum(sum(
        model.y_activity[n, t, i, o] * model.Carbon_Intensity[i, o]
        for o in model.Mode_of_operation if (i,o) in model.Carbon_Intensity) for i in model.Technology) for t in model.Time) <= model.Max_Carbon_Emission
model.CarbonEmissionLimit = pyo.Constraint(model.Nodes_in_stage, rule=Carbon_Emission_Limit)

"""

print("Objective and constraints read...")

"""
MATCHING DATA FROM CASE WITH MATHEMATICAL MODEL AND PRINTING DATA
"""
print("Building instance...")

our_model = model.create_instance(data)   
our_model.dual = pyo.Suffix(direction=pyo.Suffix.IMPORT) #Import dual values into solver results
#import pdb; pdb.set_trace()

"""
SOLVING PROBLEM
"""

import pyomo.common.tempfiles as tempfiles
import os

# Create a local temp folder for Pyomo to avoid shared /tmp conflicts
custom_tmp_dir = os.path.join(os.getcwd(), "pyomo_temp")
os.makedirs(custom_tmp_dir, exist_ok=True)
tempfiles.TempfileManager.tempdir = custom_tmp_dir


print("Solving...")

# === Create Results folder ===


opt = SolverFactory("gurobi", Verbose=True)
opt.options["Crossover"] = 0  # Set crossover value
opt.options["Method"] = 2  # Use the barrier method


# === Create Results and input folder ===
import datetime

# Generate single consistent timestamp
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# Clean filename-safe Excel name
safe_excel_path = os.path.splitext(os.path.basename(excel_path))[0].replace(" ", "_").replace("-", "_")

# Create unique folder names
results_folder = f"Results/Results_{case}_case_instance_{instance}_year_{year}_time_{timestamp}"
input_data_folder = f"Input_data/Input_{case}_case_instance_{instance}_year_{year}_time_{timestamp}"

# Create the folders
os.makedirs(results_folder, exist_ok=True)
os.makedirs(input_data_folder, exist_ok=True)


# Clean up old Gurobi log files
for f in os.listdir(results_folder):
    if f.startswith("gurobi_log_") and f.endswith(".txt"):
        os.remove(os.path.join(results_folder, f))

# Step 1: Set a temp log file
logfile_temp = os.path.join(results_folder, 'gurobi_log_temp.txt')
opt.options['LogFile'] = logfile_temp
print("✅ Created folders:")
print("  Results:", os.path.exists(results_folder))
print("  Input data:", os.path.exists(input_data_folder))


# Step 3: Copy files
input_extensions = (".tab", ".xlsx", ".csv", ".dat")
for fname in os.listdir("."):
    if os.path.isfile(fname) and not fname.endswith(".py") and fname.endswith(input_extensions):
        shutil.copy2(fname, os.path.join(input_data_folder, fname))

# Step 2: Start timing and solve
start_time = time.time()
results = opt.solve(our_model, tee=True)
end_time = time.time()
running_time = end_time - start_time

# Step 3: Rename the Gurobi log file
runtime_str = f"{running_time:.2f}s".replace('.', '_')
final_logfile = os.path.join(results_folder, f"gurobi_log_{timestamp}_{runtime_str}.txt")
os.rename(logfile_temp, final_logfile)

# Optional: Append Python timing info at the bottom
with open(final_logfile, 'a') as f:
    f.write("\n==================== PYTHON TIMING INFO ====================\n")
    f.write(f"Total solving time measured in Python: {running_time:.2f} seconds\n")


# Extract Gurobi solver information
solver_stats = results.solver

# Get the number of simplex iterations
simplex_iterations = solver_stats.statistics.number_of_iterations if hasattr(solver_stats.statistics, 'number_of_iterations') else "Unavailable"

"""
DISPLAY RESULTS??
"""
print("Writing results to .csv...")

our_model.display('results.csv')
our_model.dual.display()
print("-" * 70)
print("Objective and running time:")
print(f"Objective value: {round(pyo.value(our_model.Objective),2)}")
print(f"The instance was solved in {round(running_time, 4)} seconds🙂")
print("-" * 70)
print("Hardware details:")
print(f"Processor: {platform.processor()}")
print(f"Machine: {platform.machine()}")
print(f"System: {platform.system()} {platform.release()}")
#print(f"CPU Cores: {psutil.cpu_count(logical=True)} (Logical), {psutil.cpu_count(logical=False)} (Physical)")
#print(f"Total Memory: {psutil.virtual_memory().total / 1e9:.2f} GB")
print("-" * 70)
#import pdb; pdb.set_trace()



# === Write runtime info to .txt ===
runtime_log = f"""Solver Runtime Log
--------------------
Total Solving Time (end time - start time): {running_time:.2f} seconds

Simplex Iterations: {simplex_iterations}
"""

with open(os.path.join(results_folder, "runtime_log.txt"), "w") as f:
    f.write(runtime_log)



"""
EXTRACT VALUE OF VARIABLES AND WRITE THEM INTO EXCEL FILE
"""

print("Writing results to .xlsx...")

def save_results_to_excel(model_instance, instance, year, timestamp, max_rows_per_sheet=1_000_000):
    import pandas as pd
    from pyomo.environ import value

    filename = f"Variable_Results_instance{instance}_year{year}_{filepath}_time{timestamp}.xlsx"

    # Ensure xlsxwriter is available
    try:
        import xlsxwriter
    except ImportError:
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "xlsxwriter"])
        import site
        site.ENABLE_USER_SITE = True
        site.addsitedir(site.getusersitepackages())
        import xlsxwriter

    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        for var in model_instance.component_objects(pyo.Var, active=True):
            if var.name not in ["v_new_tech", "v_new_bat", "Not_Supplied_Energy"]:
                continue
            var_name = var.name
            var_data = []

            for index in var:
                try:
                    var_value = value(var[index])
                except:
                    continue
                #    var_value = 0
                if abs(var_value) > 1e-3:  # Only include significant values
                    var_data.append((index, var_value))

            if var_data:
                df = pd.DataFrame(var_data, columns=["Index", var_name])
                max_index_len = max(len(idx) if isinstance(idx, tuple) else 1 for idx, _ in var_data)
                unpacked = pd.DataFrame(
                    [list(idx) + [None] * (max_index_len - len(idx)) if isinstance(idx, tuple) else [idx] for idx, _ in var_data],
                    columns=[f"Index_{i+1}" for i in range(max_index_len)]
                )
                df = pd.concat([unpacked, df[var_name]], axis=1)

                for i in range(0, len(df), max_rows_per_sheet):
                    df_chunk = df.iloc[i:i + max_rows_per_sheet]
                    sheet_title = f"{var_name[:25]}_{i // max_rows_per_sheet + 1}"
                    df_chunk.to_excel(writer, sheet_name=sheet_title, index=False)

    print(f"Variable results saved to {filename}")
    return filename


# Usage after solving the model
excel_filename = save_results_to_excel(our_model, instance, year, timestamp)
shutil.move(excel_filename, os.path.join(results_folder, excel_filename))

#save_results_to_excel(our_model, filename=os.path.join(results_folder, "Variable_Results.xlsx"))


# === Write case and objective summary ===

# Get the objective value
objective_value = pyo.value(our_model.Objective)
num_Nodes = len(our_model.Nodes) if hasattr(our_model, "Nodes") else "Unknown"
num_days = len(our_model.Period)
objective_scaled_to_year = (objective_value / num_days) * 365
loadShedding_cost = pyo.value(our_model.I_loadShedding)
loadShedding_cost_scaled_to_year = (loadShedding_cost/num_days) * 365

# List of your branch counts
branches = [
    num_branches_to_firstStage,
    num_branches_to_secondStage,
    num_branches_to_thirdStage,
    num_branches_to_fourthStage,
    num_branches_to_fifthStage,
    num_branches_to_sixthStage,
    num_branches_to_seventhStage,
    num_branches_to_eighthStage,
    num_branches_to_ninthStage,
    num_branches_to_tenthStage,
    num_branches_to_eleventhStage,
    num_branches_to_twelfthStage,
    num_branches_to_thirteenthStage,
    num_branches_to_fourteenthStage,
    num_branches_to_fifteenthStage,
    ]

# Compute cumulative products, stopping when 0 is hit
cumulative = []
product = 1
for b in branches:
    if b == 0:
        break
    product *= b
    cumulative.append(product)

# Get the maximum cumulative product (number of scenarios)
num_scenarios = max(cumulative) if cumulative else 1

#print("Number of scenarios:", num_scenarios)


# Create contents
case_and_objective_content = f"""Case and Objective Summary
-----------------------------
Excel path: {excel_path}
instance: {instance}
year: {year}

Number of branches per stage:
- Stage 1: {num_branches_to_firstStage}
- Stage 2: {num_branches_to_secondStage}
- Stage 3: {num_branches_to_thirdStage}
- Stage 4: {num_branches_to_fourthStage}
- Stage 5: {num_branches_to_fifthStage}
- Stage 6: {num_branches_to_sixthStage}
- Stage 7: {num_branches_to_seventhStage}
- Stage 8: {num_branches_to_eighthStage}
- Stage 9: {num_branches_to_ninthStage}
- Stage 10: {num_branches_to_tenthStage}
- Stage 11: {num_branches_to_eleventhStage}
- Stage 12: {num_branches_to_twelfthStage}
- Stage 13: {num_branches_to_thirteenthStage}
- Stage 14: {num_branches_to_fourteenthStage}
- Stage 15: {num_branches_to_fifteenthStage}

Number of Scenarios: {num_scenarios}
Number of Nodes: {num_Nodes}
Objective Value: {objective_value:.2f}
Costs related to load shedding: {loadShedding_cost:.2f}
----------------------------------------------------
SCALED TO YEARLY COSTS:
----------------------------------------------------
Objective Value (scaled to yearly cost): {objective_scaled_to_year:.2f}
Load shedding cost (scaled to yearly cost): {loadShedding_cost_scaled_to_year:.2f}
"""

# Save it to the Results folder
with open(os.path.join(results_folder, "case_and_objective_info.txt"), "w") as f:
    f.write(case_and_objective_content)




print("Working directory:", os.getcwd())
print("Results folder will be:", results_folder)
print("Input folder will be:", input_data_folder)

"""
PLOT RESULTS
"""
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.cm import get_cmap
import numpy as np

def generate_unique_colors(n):
    cmap = plt.get_cmap("tab10")  # Use the tab10 colormap for distinct colors
    return [cmap(i % 10) for i in range(n)]

def plot_results_from_excel(input_file, output_folder, model):
    os.makedirs(output_folder, exist_ok=True)  # Create folder if it doesn't exist

    # Construct Nodes mapping dynamically
    Nodes_mapping = {n: f"Node {n}" for n in model.Nodes}

    ########################################################################################
    ############## ENDRE FOR Å DEFINERE HVILKE VARIABLER SOM IKKE SKAL PLOTTES #############
    ########################################################################################
    exclude_sheets = ["y_max", "y_activity", "Up_shift", "Dwn_Shift", "d_flex", "I_OPEX", "I_DA", "I_ID", "I_activation", "I_cap_bid", "I_inv"]
    exclude_sheets = [x.strip().lower() for x in exclude_sheets]  # Normalize sheet names

    # Read the Excel file
    excel_file = pd.ExcelFile(input_file)

    for sheet_name in excel_file.sheet_names:
        if sheet_name.strip().lower() in exclude_sheets:
            print(f"Skipping variable: {sheet_name}") 
            continue  

        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        if sheet_name in ["x_aFRR_DWN", "x_aFRR_UP"]:
            # Plot Index_1 vs second column for these sheets
            x_axis = df["Index_1"]
            y_axis = df.iloc[:, 1]  # Second column

            plt.figure(figsize=(12, 8))
            plt.plot(x_axis, y_axis, label=sheet_name, marker='o', color='blue')

            plt.title(f"{sheet_name}")
            plt.xlabel("Hours")
            plt.ylabel("Values")
            plt.legend(loc='best')
            plt.grid(True)

            plot_filename = f"{sheet_name}.png"
            plt.tight_layout()
            plt.savefig(os.path.join(output_folder, plot_filename))
            plt.close()

        elif sheet_name in ["x_aFRR_DWN_ind", "x_aFRR_UP_ind"]:
            # Handle indexed reserve market data
            if "Index_1" in df.columns and "Index_2" in df.columns:
                plt.figure(figsize=(12, 8))

                x_axis = df["Index_1"]
                value_column = df.columns[-1]
                unique_variables = df["Index_2"].dropna().unique()  # Drop NaN values
                colors = generate_unique_colors(len(unique_variables))

                for variable, color in zip(unique_variables, colors):
                    variable_data = df[df["Index_2"] == variable]
                    plt.plot(
                        variable_data["Index_1"], variable_data[value_column],
                        label=variable, marker='o', color=color
                    )

                plt.title(f"{sheet_name}")
                plt.xlabel("Hours")
                plt.ylabel("Values")
                plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=3, title="Variables", borderaxespad=0.)
                plt.grid(True)

                plot_filename = f"{sheet_name}.png"
                plt.tight_layout()
                plt.savefig(os.path.join(output_folder, plot_filename))
                plt.close()

        else:
            # General plotting for other sheets
            if "Index_1" in df.columns and "Index_2" in df.columns:
                unique_index_1 = df["Index_1"].unique()

                for index_1_value in unique_index_1:
                    filtered_df = df[df["Index_1"] == index_1_value]

                    plt.figure(figsize=(12, 8))

                    if "Index_3" in filtered_df.columns:
                        variable_column = "Index_3"
                        value_column = df.columns[-1]
                        unique_variables = filtered_df[variable_column].dropna().unique()
                        colors = generate_unique_colors(len(unique_variables))

                        for variable, color in zip(unique_variables, colors):
                            variable_data = filtered_df[filtered_df[variable_column] == variable]
                            plt.plot(
                                variable_data["Index_2"], variable_data[value_column],
                                label=variable, marker='o', color=color
                            )

                        plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=3, title="Variables", borderaxespad=0.)
                    else:
                        value_column = df.columns[-1]
                        plt.plot(filtered_df["Index_2"], filtered_df[value_column], label=value_column, marker='o', color='blue')

                    # Use Nodes mapping for title
                    Nodes_name = Nodes_mapping.get(index_1_value, f"Index_1 = {index_1_value}")
                    plt.title(f"{sheet_name} ({Nodes_name})")
                    plt.xlabel("Hours")
                    plt.ylabel("Values")
                    plt.grid(True)

                    plot_filename = f"{sheet_name}_{Nodes_name.replace(' ', '_')}.png"
                    plt.tight_layout()
                    plt.savefig(os.path.join(output_folder, plot_filename))
                    plt.close()


# Usage
if __name__ == "__main__":
    input_excel_file = "Variable_Results.xlsx"  # Path to the Excel file
    output_plots_folder = "plots"  # Folder to save the plots

    # Generate plots
    plot_results_from_excel(input_excel_file, output_plots_folder, our_model)


def extract_demand_and_flex_demand(model):
    demand_data = []
    flex_demand_data = []

    for n in model.Nodes_RT:
        for t in model.Time:
            for e in model.EnergyCarrier:
                if e == "Electricity":
                    demand_value = pyo.value(model.Demand[n, t, e])
                    flex_demand_value = pyo.value(model.d_flex[n, t, e])

                    demand_data.append({'Nodes': n, 'Time': t, 'EnergyCarrier': e, 'Reference_Demand': demand_value})
                    flex_demand_data.append({'Nodes': n, 'Time': t, 'EnergyCarrier': e, 'flex_demand': flex_demand_value})

    # Convert to DataFrame
    demand_df = pd.DataFrame(demand_data)
    flex_demand_df = pd.DataFrame(flex_demand_data)

    return demand_df, flex_demand_df
 # Get the data
demand_df, flex_demand_df = extract_demand_and_flex_demand(our_model)

# Merge the DataFrames for unified plotting
merged_df = pd.merge(demand_df, flex_demand_df, on=['Nodes', 'Time', 'EnergyCarrier'])

# Endre denne for å plotte utvalgte noder (eks. første 4 i driftsnodene)
subset_nodes = merged_df["Nodes"].unique()[:4]
subset_df = merged_df[merged_df["Nodes"].isin(subset_nodes)]

# Plotting
plt.figure(figsize=(12, 6))

#####################################################################################
########################### FOR Å PLOTTE ALLE NODENE ################################
#####################################################################################

#for Nodes in merged_df['Nodes'].unique():
#    Nodes_data = merged_df[merged_df['Nodes'] == Nodes]
#    plt.step(Nodes_data['Time'], Nodes_data['Reference_Demand'],label=f'Demand - Nodes {Nodes}')
#    plt.step(Nodes_data['Time'], Nodes_data['flex_demand'], "--", label=f'Flex Demand - Nodes {Nodes}')


#####################################################################################
########################### FOR Å PLOTTE UTVALGTE NODER #############################
#####################################################################################
for node in subset_nodes:
    node_data = subset_df[subset_df["Nodes"] == node]
    plt.step(node_data["Time"], node_data["Reference_Demand"], label=f"Ref Demand - Node {node}", linestyle="-")
    plt.step(node_data["Time"], node_data["flex_demand"], label=f"Flex Demand - Node {node}", linestyle="--")


plt.xlabel('Time')
plt.ylabel('Demand (MW)')
plt.title('Demand and Flexible Demand Over Time')
plt.legend()
plt.grid(True)
plt.show()
"""