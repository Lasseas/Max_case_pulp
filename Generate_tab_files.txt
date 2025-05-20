import pandas as pd
import numpy as np
import random
from Get_Historical_and_technological_data_file import Cost_export, ReferenceDemand, NodeProbability, Tech_availability, CapacityUpPrice, CapacityDwnPrice, ActivationUpPrice, ActivationDwnPrice, SpotPrice, IntradayPrice, CostExpansion_Tec, CostExpansion_Bat, CostGridTariff, Res_CapacityDwnVolume, Res_CapacityUpVolume, ID_Capacity_Buy_Volume, ID_Capacity_Sell_Volume, parent_mapping, excel_path


#####################################################################################
################################## KONSTANTE SETT ###################################
#####################################################################################
#################### HUSK Å ENDRE DISSE I DE ANDRE FILENE OGSÅ ######################
#####################################################################################
#excel_path = "NO1_Aluminum_2024_combined historical data.xlsx"
excel_path = "NO1_Pulp_Paper_2024_combined historical data.xlsx"
instance = 1                    # state which instance you would like to run for
year = 2025                     # state which year you would like to run for

num_branches_to_firstStage = 2 # Antall grener til det vi i LateX har definert som Omega^first
num_branches_to_secondStage = 45
num_branches_to_thirdStage = 45
num_branches_to_fourthStage = 0
num_branches_to_fifthStage = 0
num_branches_to_sixthStage = 0
num_branches_to_seventhStage = 0
num_branches_to_eighthStage = 0
num_branches_to_ninthStage = 0
num_branches_to_tenthStage = 0


num_timesteps = 24
num_nodes = num_branches_to_firstStage + num_branches_to_firstStage*num_branches_to_secondStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage
num_firstStageNodes = num_branches_to_firstStage
num_nodesInlastStage = max(num_branches_to_firstStage, num_branches_to_firstStage*num_branches_to_secondStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage)


technologies = ["Power_Grid", "ElectricBoiler", "HP_LT", "HP_MT", "PV", "P2G", "G2P", "GasBoiler", "GasBoiler_CCS", "CHP", "CHP_CCS", "Biogas_Grid", "CH4_Grid", "CH4_H2_Mixer", "DieselReserveGenerator", "H2_Grid"]
energy_carriers = ["Electricity", "LT", "MT", "H2", "CH4", "Biogas", "CH4_H2_Mix"]
StorageTech = ["BESS_Li_Ion_1", "BESS_Redox_1", "CEAS_1", "Flywheel_1", "Hot_Water_Tank_LT_1", "H2_Storage_1", "CH4_Storage_1"]

Cost_energy = {
    "Power_Grid": 0,
    "ElectricBoiler": 0,
    "HP_LT": 0,
    "HP_MT": 0,
    "PV": 0,
    "P2G": 0,
    "G2P": 0,
    "GasBoiler": 0,
    "GasBoiler_CCS": 0,
    "CHP": 0,
    "CHP_CCS": 0,
    "Biogas_Grid": 64.5, #dobbeltsjekk
    "CH4_Grid": 39.479, #dobbeltsjekk
    "CH4_H2_Mixer": 0,
    "DieselReserveGenerator": 148.8,
    "H2_Grid": 150.1502
}




    

#####################################################################################
########################### FUNKSJON FOR Å LAGE .tab-FIL #############################
#####################################################################################

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




#####################################################################################
########################### SET GENERATION FUNCTIONS ################################
#####################################################################################
def generate_Set_TimeSteps(num_timesteps, filename = "Set_of_TimeSteps.tab"):
    def data_generator(chunk_size=10_000_000):
        # Create a DataFrame with a single column "Time" containing time steps 1 to num_timesteps.
        df = pd.DataFrame({"Time": range(1, num_timesteps + 1)})
        yield df

    make_tab_file(filename, data_generator())


def generate_Set_of_Nodes(num_nodes, filename = "Set_of_Nodes.tab"):
    def data_generator(chunk_size=10_000_000):
        # Create a DataFrame with a single column "Node" containing node numbers 1 to num_nodes.
        df = pd.DataFrame({"Nodes": range(1, num_nodes + 1)})
        yield df

    make_tab_file(filename, data_generator())


def generate_set_of_NodesFirst(num_branches_to_firstStage, filename = "Subset_NodesFirst.tab"):
    def data_generator(chunk_size=10_000_000):
        # Create a DataFrame with a single column "Node" containing node numbers 1 to num_nodes.
        df = pd.DataFrame({"Nodes": range(1, num_branches_to_firstStage + 1)})
        yield df

    make_tab_file(filename, data_generator())


def generate_set_of_Parents(num_nodes, filename = "Set_of_Parents.tab"):
    def data_generator(chunk_size=10_000_000):
        df = pd.DataFrame({"Parent": range(1, num_nodes - num_nodesInlastStage + 1)})
        yield df
    
    make_tab_file(filename, data_generator())


def generate_set_Parent_Coupling(list_of_branches, filename = "Set_ParentCoupling.tab"):
    parent_mapping = []  # To store rows with "Node" and "Parent"
    
    # Stage 1: The root nodes (these do not appear in the output, as they have no parent)
    current_stage = list(range(1, list_of_branches[0] + 1))
    node_counter = current_stage[-1]  # Last node number in stage 1

    # For each subsequent stage, generate children for each node in the current stage.
    # The branch_counts list has one entry per stage; stage 1 is already defined.
    for stage_index in range(1, len(list_of_branches)):
        next_stage = []
        branches = list_of_branches[stage_index]  # Number of children per parent for this stage
        for parent in current_stage:
            for _ in range(branches):
                node_counter += 1
                child = node_counter
                parent_mapping.append({"Node": child, "Parent": parent})
                next_stage.append(child)
        current_stage = next_stage  # Update for the next stage

    def data_generator(chunk_size=10_000_000):
        # In this example, the entire mapping is yielded as one chunk.
        yield pd.DataFrame(parent_mapping)
    
    make_tab_file(filename, data_generator())


def generate_set_of_NodesInStage(branch_counts, filename = "Set_of_NodesInStage.tab"):
    nodes_in_stage = []  # To hold rows of the form {"Nodes": child_node, "Period": period}
    
    # Define the root nodes (stage 1) – these are not output since they have no parent period.
    current_stage = list(range(1, branch_counts[0] + 1))
    node_counter = current_stage[-1]  # Last node number in stage 1
    
    # For each subsequent stage, generate children and record their period (stage_index).
    # Here, stage 2 corresponds to Period 1, stage 3 to Period 2, etc.
    for stage_index in range(1, len(branch_counts)):
        next_stage = []
        period = stage_index  # period = stage_index (so stage 2 -> period 1, stage 3 -> period 2, etc.)
        for parent in current_stage:
            for _ in range(branch_counts[stage_index]):
                node_counter += 1
                child = node_counter
                next_stage.append(child)
                nodes_in_stage.append({"Nodes": child, "Period": period})
        current_stage = next_stage

    def data_generator(chunk_size=10_000_000):
        # Yield the entire mapping as one chunk.
        yield pd.DataFrame(nodes_in_stage)
    
    make_tab_file(filename, data_generator())


def generate_set_of_Periods(branch_counts, filename = "Set_of_Periods.tab"):
    def data_generator(chunk_size=10_000_000):
        # Consider stages 2 and beyond (i.e. branch_counts[1:]) and count only those > 0.
        valid_periods = [i for i, count in enumerate(branch_counts[1:], start=1) if count != 0]
        # Create a DataFrame with valid period numbers.
        df = pd.DataFrame({"Periods": valid_periods})
        yield df

    make_tab_file(filename, data_generator())

#####################################################################################
########################### PARAMETER GENERATION FUNCTIONS ##########################
#####################################################################################

# Function to count number of periods from Set_of_Periods.tab
def get_number_of_periods_from_tab(filepath="Set_of_Periods.tab"):
    df = pd.read_csv(filepath, sep="\t")
    return len(df)
def generate_set_of_LoadShiftingPeriod(periods_tab="Set_of_Periods.tab", filename="Set_of_LoadShiftingPeriod.tab"):
    def data_generator():
        # Read the periods tab file
        df_periods = pd.read_csv(periods_tab, sep="\t")
        
        # Get the maximum period (last one)
        last_period = df_periods["Periods"].max()
        
        # Create a new DataFrame with only that last period
        df_last = pd.DataFrame({"LoadShiftingPeriod": [last_period]})
        
        yield df_last

    # Use the make_tab_file function to write it
    make_tab_file(filename, data_generator())

def generate_set_of_PeriodsInMonth(branch_counts, filename="Set_of_PeriodsInMonth.tab"):
    def data_generator():
        # Extract valid period indices (starting from stage 2 = index 1)
        valid_periods = [i for i, count in enumerate(branch_counts[1:], start=1) if count > 0]

        # Create the DataFrame assigning all periods to Month 1
        df = pd.DataFrame({
            "Month": [1] * len(valid_periods),
            "PeriodInMonth": list(range(1, len(valid_periods) + 1))
        })

        yield df

    make_tab_file(filename, data_generator())

# Functions to scale and write .tab files for each parameter
def generate_Par_CostExpansion_Tec(filename="Par_CostExpansion_Tec.tab"):
    num_periods = get_number_of_periods_from_tab()
    rows = [
        {"Technology": tech, "CostExpansion": cost * num_periods}
        for tech, cost in CostExpansion_Tec.items()
    ]
    df = pd.DataFrame(rows)
    df.to_csv(filename, sep="\t", index=False, lineterminator='\n')
    print(f"{filename} saved successfully!")

def generate_Par_CostExpansion_Bat(filename="Par_CostExpansion_Bat.tab"):
    num_periods = get_number_of_periods_from_tab()
    rows = [
        {"StorageTech": bat, "CostExpansion": cost * num_periods}
        for bat, cost in CostExpansion_Bat.items()
    ]
    df = pd.DataFrame(rows)
    df.to_csv(filename, sep="\t", index=False, lineterminator='\n')
    print(f"{filename} saved successfully!")

def generate_Par_CostGridTariff(filename="Par_CostGridTariff.tab"):
    num_periods = get_number_of_periods_from_tab()
    total_tariff = CostGridTariff * num_periods

    df = pd.DataFrame([{
        "Tariff": total_tariff
    }])

    df.to_csv(filename, sep="\t", index=False, lineterminator='\n')
    print(f"{filename} saved successfully!")


def generate_Par_LastPeriodInMonth(filename="Par_LastPeriodInMonth.tab"):
    num_periods = get_number_of_periods_from_tab()
    df = pd.DataFrame([{"Month": 1, "LastPeriodInMonth": num_periods}])
    df.to_csv(filename, sep="\t", index=False, lineterminator='\n')
    print(f"{filename} saved successfully!")


# Call them after Set_of_Periods.tab is created
generate_Par_CostExpansion_Tec()
generate_Par_CostExpansion_Bat()
generate_Par_CostGridTariff()
generate_Par_LastPeriodInMonth()

##########
## Additional Parameter generation functions ###
##########


def generate_cost_energy(num_nodes, num_timesteps, technologies, cost_energy, filename = "Par_EnergyCost.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        for node in range(num_firstStageNodes + 1, num_nodes + 1):
            for tech in technologies:
                for t in range(1, num_timesteps + 1):
                    # If cost_energy is a dict, get the cost for the current technology.
                    # Otherwise, assume cost_energy is a constant value.
                    cost = cost_energy.get(tech, 0.0) if isinstance(cost_energy, dict) else cost_energy
                    rows.append({"Node": node, "Time": t, "Technology": tech, "Cost": cost})
                    count += 1
                    if count % chunk_size == 0:
                        yield pd.DataFrame(rows)
                        rows = []
        if rows:
            yield pd.DataFrame(rows)

    make_tab_file(filename, data_generator())


def generate_cost_export(num_nodes, num_timesteps, technologies, cost_export, filename = "Par_ExportCost.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        for node in range(num_firstStageNodes + 1, num_nodes + 1):
            for tech in technologies:
                for t in range(1, num_timesteps + 1):
                    tech_cost = cost_export.get(tech, 0.0)
                    if isinstance(tech_cost, dict) and (node in tech_cost):
                        node_cost = tech_cost[node]
                        if isinstance(node_cost, dict):
                            cost = node_cost.get(t, 0.0)
                        elif isinstance(node_cost, list):
                            cost = node_cost[t - 1] if len(node_cost) >= t else 0.0
                        else:
                            cost = node_cost
                    else:
                        if isinstance(tech_cost, dict):
                            cost = tech_cost.get(t, 0.0)
                        elif isinstance(tech_cost, list):
                            cost = tech_cost[t - 1] if len(tech_cost) >= t else 0.0
                        else:
                            cost = tech_cost
                    rows.append({"Node": node, "Time": t, "Technology": tech, "CostExport": cost})
                    count += 1
                    if count % chunk_size == 0:
                        yield pd.DataFrame(rows)
                        rows = []
        if rows:
            yield pd.DataFrame(rows)
    make_tab_file(filename, data_generator())


def generate_CapacityUpPrice(num_nodes, num_timesteps, CapacityUpPrice, filename = "Par_aFRR_UP_CAP_price.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        for node in range(1, num_nodes - num_nodesInlastStage + 1):
            # For a given node, retrieve its time-dependent prices (defaults to an empty dict if not found)
            node_prices = CapacityUpPrice.get(node, {})
            for t in range(1, num_timesteps + 1):
                price = node_prices.get(t, 0.0)
                rows.append({"Node": node, "Time": t, "CapacityUpPrice": price})
                count += 1
                if count % chunk_size == 0:
                    yield pd.DataFrame(rows)
                    rows = []
        if rows:
            yield pd.DataFrame(rows)
    
    make_tab_file(filename, data_generator())

def generate_CapacityDownPrice(num_nodes, num_timesteps, CapacityDownPrice, filename = "Par_aFRR_DWN_CAP_price.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        for node in range(1, num_nodes - num_nodesInlastStage + 1):
            # For a given node, retrieve its time-dependent prices (defaults to an empty dict if not found)
            node_prices = CapacityDownPrice.get(node, {})
            for t in range(1, num_timesteps + 1):
                price = node_prices.get(t, 0.0)
                rows.append({"Node": node, "Time": t, "CapacityDownPrice": price})
                count += 1
                if count % chunk_size == 0:
                    yield pd.DataFrame(rows)
                    rows = []
        if rows:
            yield pd.DataFrame(rows)
    
    make_tab_file(filename, data_generator())


def generate_ActivationUpPrice(num_nodes, num_timesteps, ActivationUpPrice, filename = "Par_aFRR_UP_ACT_price.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        for node in range(num_firstStageNodes + 1, num_nodes + 1):
            # Retrieve the time-dependent activation up prices for the current node (defaults to an empty dict if not found)
            node_prices = ActivationUpPrice.get(node, {})
            for t in range(1, num_timesteps + 1):
                price = node_prices.get(t, 0.0)
                rows.append({"Node": node, "Time": t, "ActivationUpPrice": price})
                count += 1
                if count % chunk_size == 0:
                    yield pd.DataFrame(rows)
                    rows = []
        if rows:
            yield pd.DataFrame(rows)
    
    make_tab_file(filename, data_generator())

def generate_ActivationDownPrice(num_nodes, num_timesteps, ActivationDownPrice, filename = "Par_aFRR_DWN_ACT_price.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        for node in range(num_firstStageNodes + 1, num_nodes + 1):
            # Retrieve the time-dependent activation down prices for the current node (defaults to an empty dict if not found)
            node_prices = ActivationDownPrice.get(node, {})
            for t in range(1, num_timesteps + 1):
                price = node_prices.get(t, 0.0)
                rows.append({"Node": node, "Time": t, "ActivationDownPrice": price})
                count += 1
                if count % chunk_size == 0:
                    yield pd.DataFrame(rows)
                    rows = []
        if rows:
            yield pd.DataFrame(rows)
    
    make_tab_file(filename, data_generator())

def generate_SpotPrice(num_nodes, num_timesteps, SpotPrice, filename = "Par_SpotPrice.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        for node in range(num_firstStageNodes + 1, num_nodes + 1):
            # Retrieve time-dependent spot prices for the current node (defaults to an empty dict if not found)
            node_prices = SpotPrice.get(node, {})
            for t in range(1, num_timesteps + 1):
                price = node_prices.get(t, 0.0)
                rows.append({"Node": node, "Time": t, "Spot_Price": price})
                count += 1
                if count % chunk_size == 0:
                    yield pd.DataFrame(rows)
                    rows = []
        if rows:
            yield pd.DataFrame(rows)
    
    make_tab_file(filename, data_generator())

def generate_IntradayPrice(num_nodes, num_timesteps, IntradayPrice, filename = "Par_IntradayPrice.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        for node in range(num_firstStageNodes + 1, num_nodes + 1):
            # Retrieve time-dependent spot prices for the current node (defaults to an empty dict if not found)
            node_prices = IntradayPrice.get(node, {})
            for t in range(1, num_timesteps + 1):
                price = node_prices.get(t, 0.0)
                rows.append({"Node": node, "Time": t, "Intraday_Price": price})
                count += 1
                if count % chunk_size == 0:
                    yield pd.DataFrame(rows)
                    rows = []
        if rows:
            yield pd.DataFrame(rows)
    
    make_tab_file(filename, data_generator())


def generate_ReferenceDemand(num_nodes, num_timesteps, energy_carriers, ReferenceDemand, filename = "Par_EnergyDemand.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        # Loop over nodes (using the same range as cost_export for consistency)
        for node in range(num_firstStageNodes + 1, num_nodes + 1):
            for ec in energy_carriers:
                for t in range(1, num_timesteps + 1):
                    ec_value = ReferenceDemand.get(ec, 0.0)
                    # If ec_value is a dict and contains a node-specific entry, use that
                    if isinstance(ec_value, dict) and (node in ec_value):
                        node_value = ec_value[node]
                        if isinstance(node_value, dict):
                            demand = node_value.get(t, 0.0)
                        elif isinstance(node_value, list):
                            demand = node_value[t - 1] if len(node_value) >= t else 0.0
                        else:
                            demand = node_value
                    else:
                        # Otherwise treat ec_value as time-dependent or constant
                        if isinstance(ec_value, dict):
                            demand = ec_value.get(t, 0.0)
                        elif isinstance(ec_value, list):
                            demand = ec_value[t - 1] if len(ec_value) >= t else 0.0
                        else:
                            demand = ec_value
                    rows.append({"Node": node, "Time": t, "EnergyCarrier": ec, "ReferenceDemand": demand})
                    count += 1
                    if count % chunk_size == 0:
                        yield pd.DataFrame(rows)
                        rows = []
        if rows:
            yield pd.DataFrame(rows)
    make_tab_file(filename, data_generator())

def generate_NodeProbability(num_nodes, NodeProbability, filename = "Par_NodesProbability.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        # Loop over each node (assumed to be numbered 1 to num_nodes)
        for node in range(1, num_nodes + 1):
            prob = NodeProbability.get(node, 0.0)
            rows.append({"Node": node, "NodeProbability": prob})
            count += 1
            if count % chunk_size == 0:
                yield pd.DataFrame(rows)
                rows = []
        if rows:
            yield pd.DataFrame(rows)
    make_tab_file(filename, data_generator())

def generate_availability_factor(num_nodes, num_timesteps, technologies, tech_availability, filename="Par_AvailabilityFactor.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        # Note: The following loop starts at num_firstStageNodes + 1.
        # If not needed, you can replace the range with: range(1, num_nodes + 1)
        for node in range(num_firstStageNodes + 1, num_nodes + 1):
            for tech in technologies:
                for t in range(1, num_timesteps + 1):
                    if tech == 'PV' and isinstance(tech_availability.get(tech), dict):
                        # For PV, retrieve the node- and time-specific factor.
                        avail = tech_availability[tech].get(node, {}).get(t, 0.0)
                    else:
                        # For other technologies, use the constant value.
                        avail = tech_availability.get(tech, 0.0)
                    rows.append({
                        "Node": node,
                        "Time": t,
                        "Technology": tech,
                        "AvailabilityFactor": avail
                    })
                    count += 1
                    if count % chunk_size == 0:
                        yield pd.DataFrame(rows)
                        rows = []
        if rows:
            yield pd.DataFrame(rows)

    make_tab_file(filename, data_generator())



def generate_activation_factors(num_nodes, num_timesteps, parent_mapping, activation_rate=0.8):
    
    #activation_rate = andel av barnenodene som skal være aktive (enten opp eller ned) i hver time.
    
    parent_to_children = {}
    for child, parent in parent_mapping.items():
        parent_to_children.setdefault(parent, []).append(child)

    rows = []

    for parent, children in parent_to_children.items():
        for t in range(1, num_timesteps + 1):
            num_children = len(children)
            if num_children < 2:
                raise ValueError(f"Forelder {parent} har for få barn til å sikre både opp- og nedregulering i time {t}")

            # Hvor mange barn skal aktiveres denne timen?
            num_active = max(2, int(activation_rate * num_children))  # minst 2 (én opp, én ned)
            
            active_children = random.sample(children, num_active)

            # Plukk én for opp, én for ned
            random.shuffle(active_children)
            child_up = active_children.pop()
            child_down = active_children.pop()

            activation = {}

            for child in children:
                if child == child_up:
                    activation[child] = (1, 0)
                elif child == child_down:
                    activation[child] = (0, 1)
                elif child in active_children:
                    # Fordel tilfeldig opp eller ned på resten av de aktive
                    if random.random() < 0.5:
                        activation[child] = (1, 0)
                    else:
                        activation[child] = (0, 1)
                else:
                    # Ikke aktivert
                    activation[child] = (0, 0)

            for child, (up, down) in activation.items():
                rows.append({
                    "Node": child,
                    "Time": t,
                    "ActivationFactorUpReg": up,
                    "ActivationFactorDownReg": down
                })

    return pd.DataFrame(rows)

################################################################################################
########################### ACTIVATION FACTORS FOR ALU-INDUSTRY ################################
################################################################################################
# Krever ca. 50 branches for 30% aktiveringsrate med 8t hviletid - Færre branches krever activation_rate.
# For få branches vil gi en feilmelding (For få tilgjengelige barn...), så bare å prøve seg frem:)

def generate_activation_factors_with_rest_time(num_nodes, num_timesteps, parent_mapping, activation_rate=0.30, rest_hours=8):
   
    #Generate activation factors with 8 hours rest after activation.
    
    parent_to_children = {}
    for child, parent in parent_mapping.items():
        parent_to_children.setdefault(parent, []).append(child)

    rows = []
    
    # Track when each child node becomes available again
    available_from = {child: 1 for child in parent_mapping.keys()}  # Starter som tilgjengelig fra time 1

    for parent, children in parent_to_children.items():
        for t in range(1, num_timesteps + 1):
            # Filter children that are available at time t
            available_children = [child for child in children if available_from[child] <= t]

            if len(available_children) < 2:
                raise ValueError(f"For få tilgjengelige barn for forelder {parent} ved time {t} for å sikre aktivering.")

            # Hvor mange skal aktiveres
            num_children = len(available_children)
            num_active = max(2, int(activation_rate * num_children))  # minst 2

            active_children = random.sample(available_children, min(num_active, num_children))

            random.shuffle(active_children)
            child_up = active_children.pop()
            child_down = active_children.pop()

            activation = {}

            for child in children:
                if child == child_up:
                    activation[child] = (1, 0)
                    available_from[child] = t + rest_hours + 1  # Låst i 8 timer etter aktivering
                elif child == child_down:
                    activation[child] = (0, 1)
                    available_from[child] = t + rest_hours + 1
                elif child in active_children:
                    if random.random() < 0.5:
                        activation[child] = (1, 0)
                    else:
                        activation[child] = (0, 1)
                    available_from[child] = t + rest_hours + 1
                else:
                    activation[child] = (0, 0)

            # Logg aktiveringer
            for child, (up, down) in activation.items():
                rows.append({
                    "Node": child,
                    "Time": t,
                    "ActivationFactorUpReg": up,
                    "ActivationFactorDownReg": down
                })

    return pd.DataFrame(rows)



def generate_joint_regulation_activation_files(num_nodes, num_timesteps, up_filename = "Par_ActivationFactor_Up_Reg.tab", down_filename = "Par_ActivationFactor_Dwn_Reg.tab"):
    if excel_path == "NO1_Pulp_Paper_2024_combined historical data.xlsx":
        df_joint = generate_activation_factors(num_nodes, num_timesteps, parent_mapping)
    elif excel_path == "NO1_Aluminum_2024_combined historical data.xlsx":
        df_joint = generate_activation_factors_with_rest_time(num_nodes, num_timesteps, parent_mapping, activation_rate=0.30, rest_hours=8)
    else:
        raise ValueError("Invalid excel_path. Please provide a valid path.")

    # Write UpReg file
    def data_generator_up():
        df_up = df_joint.rename(columns={"ActivationFactorUpReg": "ActivationFactorUpRegulation"})
        yield df_up[["Node", "Time", "ActivationFactorUpRegulation"]]

    make_tab_file(up_filename, data_generator_up())

    # Write DownReg file
    def data_generator_down():
        df_down = df_joint.rename(columns={"ActivationFactorDownReg": "ActivationFactorDwnRegulation"})
        yield df_down[["Node", "Time", "ActivationFactorDwnRegulation"]]

    make_tab_file(down_filename, data_generator_down())



def generate_ID_factors(num_nodes, num_timesteps, p_id_up=0.4, p_id_down=0.4):
    rows = []
    # Process nodes beyond the first-stage nodes.
    for node in range(num_firstStageNodes + 1, num_nodes + 1):
        for t in range(1, num_timesteps + 1):
            # Independent draws for ID up and ID down.
            up = 1 if random.random() < p_id_up else 0
            down = 1 if random.random() < p_id_down else 0
            
            rows.append({
                "Node": node,
                "Time": t,
                "ActivationFactorID_UP": up,
                "ActivationFactorID_Dwn": down
            })
    return pd.DataFrame(rows)

def generate_ActivationFactorID_UP(num_nodes, num_timesteps, p_id_up = 0.3, p_id_down = 0.2, filename = "Par_ActivationFactor_ID_Up_Reg.tab"):
    def data_generator(chunk_size=10_000_000):
        df_joint = generate_ID_factors(num_nodes, num_timesteps, p_id_up, p_id_down)
        # Rename column for clarity
        df_joint = df_joint.rename(columns={"ActivationFactorID_UP": "ActivationFactorID_UP_Reg"})
        yield df_joint[["Node", "Time", "ActivationFactorID_UP_Reg"]]
    
    make_tab_file(filename, data_generator())

def generate_ActivationFactorID_Dwn(num_nodes, num_timesteps, p_id_up = 0.3, p_id_down = 0.2, filename = "Par_ActivationFactor_ID_Dwn_Reg.tab"):
    def data_generator(chunk_size=10_000_000):
        df_joint = generate_ID_factors(num_nodes, num_timesteps, p_id_up, p_id_down)
        df_joint = df_joint.rename(columns={"ActivationFactorID_Dwn": "ActivationFactorID_Dwn_Reg"})
        yield df_joint[["Node", "Time", "ActivationFactorID_Dwn_Reg"]]
    
    make_tab_file(filename, data_generator())

def generate_Res_CapacityUpVolume(num_nodes, num_timesteps, Res_CapacityUpVolume, filename="Par_Res_CapacityUpVolume.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        for node in range(1, num_nodes + 1):
            node_data = Res_CapacityUpVolume.get(node, {})
            for t in range(1, num_timesteps + 1):
                volume = node_data.get(t, 0.0)
                rows.append({"Node": node, "Time": t, "Res_CapacityUpVolume": volume})
                count += 1
                if count % chunk_size == 0:
                    yield pd.DataFrame(rows)
                    rows = []
        if rows:
            yield pd.DataFrame(rows)
    make_tab_file(filename, data_generator())


def generate_Res_CapacityDownVolume(num_nodes, num_timesteps, Res_CapacityDwnVolume, filename="Par_Res_CapacityDownVolume.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        for node in range(1, num_nodes + 1):
            node_data = Res_CapacityDwnVolume.get(node, {})
            for t in range(1, num_timesteps + 1):
                volume = node_data.get(t, 0.0)
                rows.append({"Node": node, "Time": t, "Res_CapacityDownVolume": volume})
                count += 1
                if count % chunk_size == 0:
                    yield pd.DataFrame(rows)
                    rows = []
        if rows:
            yield pd.DataFrame(rows)
    make_tab_file(filename, data_generator())

def generate_ID_Capacity_Sell_Volume(num_nodes, num_timesteps, Res_CapacityDwnVolume, filename="Par_ID_Capacity_Sell_Volume.tab"):
    def data_generator(chunk_size=10_000_000):
        rows = []
        count = 0
        for node in range(1, num_nodes + 1):
            node_data = ID_Capacity_Sell_Volume.get(node, {})
            for t in range(1, num_timesteps + 1):
                volume = node_data.get(t, 0.0)
                rows.append({"Node": node, "Time": t, "ID_Capacity_Sell_Volume": volume})
                count += 1
                if count % chunk_size == 0:
                    yield pd.DataFrame(rows)
                    rows = []
        if rows:
            yield pd.DataFrame(rows)
    make_tab_file(filename, data_generator())

def generate_ID_Capacity_Buy_Volume(num_nodes, num_timesteps, Res_CapacityDwnVolume, filename="Par_ID_Capacity_Buy_Volume.tab"):
        def data_generator(chunk_size=10_000_000):
            rows = []
            count = 0
            for node in range(1, num_nodes + 1):
                node_data = ID_Capacity_Buy_Volume.get(node, {})
                for t in range(1, num_timesteps + 1):
                    volume = node_data.get(t, 0.0)
                    rows.append({"Node": node, "Time": t, "ID_Capacity_Buy_Volume": volume})
                    count += 1
                    if count % chunk_size == 0:
                        yield pd.DataFrame(rows)
                        rows = []
            if rows:
                yield pd.DataFrame(rows)
        make_tab_file(filename, data_generator())



##########################################################################
########################### GENERATE SETS ################################
##########################################################################
generate_Set_TimeSteps(num_timesteps)
generate_Set_of_Nodes(num_nodes)
generate_set_of_Parents(num_nodes)
generate_set_Parent_Coupling([num_branches_to_firstStage, num_branches_to_secondStage, num_branches_to_thirdStage, num_branches_to_fourthStage, num_branches_to_fifthStage, num_branches_to_sixthStage, num_branches_to_seventhStage, num_branches_to_eighthStage, num_branches_to_ninthStage, num_branches_to_tenthStage])
generate_set_of_NodesInStage([num_branches_to_firstStage, num_branches_to_secondStage, num_branches_to_thirdStage, num_branches_to_fourthStage, num_branches_to_fifthStage, num_branches_to_sixthStage, num_branches_to_seventhStage, num_branches_to_eighthStage, num_branches_to_ninthStage, num_branches_to_tenthStage])
generate_set_of_Periods([num_branches_to_firstStage, num_branches_to_secondStage, num_branches_to_thirdStage, num_branches_to_fourthStage, num_branches_to_fifthStage, num_branches_to_sixthStage, num_branches_to_seventhStage, num_branches_to_eighthStage, num_branches_to_ninthStage, num_branches_to_tenthStage])
generate_set_of_PeriodsInMonth([num_branches_to_firstStage, num_branches_to_secondStage, num_branches_to_thirdStage, num_branches_to_fourthStage, num_branches_to_fifthStage, num_branches_to_sixthStage, num_branches_to_seventhStage, num_branches_to_eighthStage, num_branches_to_ninthStage, num_branches_to_tenthStage])
generate_set_of_LoadShiftingPeriod()
generate_set_of_NodesFirst(num_branches_to_firstStage)

##########################################################################
########################### GENERATE PARAMETERS ##########################
##########################################################################

generate_cost_energy(num_nodes, num_timesteps, technologies, Cost_energy)
generate_cost_export(num_nodes, num_timesteps, technologies, Cost_export)
generate_CapacityUpPrice(num_nodes, num_timesteps, CapacityUpPrice)
generate_CapacityDownPrice(num_nodes, num_timesteps, CapacityDwnPrice)
generate_ActivationUpPrice(num_nodes, num_timesteps, ActivationUpPrice)
generate_ActivationDownPrice(num_nodes, num_timesteps, ActivationDwnPrice)
generate_SpotPrice(num_nodes, num_timesteps, SpotPrice)
generate_IntradayPrice(num_nodes, num_timesteps, IntradayPrice)
generate_ReferenceDemand(num_nodes, num_timesteps, energy_carriers, ReferenceDemand)
generate_NodeProbability(num_nodes, NodeProbability)
generate_availability_factor(num_nodes, num_timesteps, technologies, Tech_availability)
generate_joint_regulation_activation_files(num_nodes, num_timesteps)
generate_ActivationFactorID_UP(num_nodes, num_timesteps, p_id_up=0.3, p_id_down=0.2)
generate_ActivationFactorID_Dwn(num_nodes, num_timesteps, p_id_up=0.3, p_id_down=0.2)
generate_Res_CapacityDownVolume(num_nodes, num_timesteps, Res_CapacityDwnVolume)
generate_Res_CapacityUpVolume(num_nodes, num_timesteps, Res_CapacityUpVolume)
generate_ID_Capacity_Sell_Volume(num_nodes, num_timesteps, ID_Capacity_Sell_Volume)
generate_ID_Capacity_Buy_Volume(num_nodes, num_timesteps, ID_Capacity_Buy_Volume)
