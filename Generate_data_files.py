import pandas as pd
import numpy as np
import random

def run_everything(excel_path, instance, year, num_branches_to_firstStage, num_branches_to_secondStage, num_branches_to_thirdStage, num_branches_to_fourthStage, num_branches_to_fifthStage, num_branches_to_sixthStage, num_branches_to_seventhStage, num_branches_to_eighthStage, num_branches_to_ninthStage, num_branches_to_tenthStage, num_branches_to_eleventhStage, num_branches_to_twelfthStage, num_branches_to_thirteenthStage, num_branches_to_fourteenthStage, num_branches_to_fifteenthStage):
    

    num_timesteps = 24
    num_nodes = (
        num_branches_to_firstStage + num_branches_to_firstStage*num_branches_to_secondStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage 
        + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage*num_branches_to_eleventhStage 
        + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage*num_branches_to_eleventhStage*num_branches_to_twelfthStage 
        + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage*num_branches_to_eleventhStage*num_branches_to_twelfthStage*num_branches_to_thirteenthStage 
        + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage*num_branches_to_eleventhStage*num_branches_to_twelfthStage*num_branches_to_thirteenthStage*num_branches_to_fourteenthStage
        + num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage*num_branches_to_eleventhStage*num_branches_to_twelfthStage*num_branches_to_thirteenthStage*num_branches_to_fourteenthStage*num_branches_to_fifteenthStage
    )
    num_firstStageNodes = num_branches_to_firstStage
    num_nodesInlastStage = max(num_branches_to_firstStage, num_branches_to_firstStage*num_branches_to_secondStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage*num_branches_to_eleventhStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage*num_branches_to_eleventhStage*num_branches_to_twelfthStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage*num_branches_to_eleventhStage*num_branches_to_twelfthStage*num_branches_to_thirteenthStage, num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage*num_branches_to_eleventhStage*num_branches_to_twelfthStage*num_branches_to_thirteenthStage*num_branches_to_fourteenthStage,num_branches_to_firstStage*num_branches_to_secondStage*num_branches_to_thirdStage*num_branches_to_fourthStage*num_branches_to_fifthStage*num_branches_to_sixthStage*num_branches_to_seventhStage*num_branches_to_eighthStage*num_branches_to_ninthStage*num_branches_to_tenthStage*num_branches_to_eleventhStage*num_branches_to_twelfthStage*num_branches_to_thirteenthStage*num_branches_to_fourteenthStage*num_branches_to_fifteenthStage)


    technologies = ["Power_Grid", "ElectricBoiler", "HP_LT", "HP_MT", "PV", "P2G", "G2P", "GasBoiler", "GasBoiler_CCS", "CHP", "CHP_CCS", "Biogas_Grid", "CH4_Grid", "CH4_H2_Mixer", "DieselReserveGenerator", "H2_Grid", "Dummy_Grid"]
    energy_carriers = ["Electricity", "LT", "MT", "H2", "CH4", "Biogas", "CH4_H2_Mix", "DummyFuel"]
    StorageTech = ["BESS_Li_Ion_1", "BESS_Redox_1", "CEAS_1", "Flywheel_1", "Hot_Water_Tank_LT_1", "H2_Storage_1", "CH4_Storage_1"]

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

    ###############################################################################
    ################# DENNE MÅ LAGES TIDLIGERE ENN DE ANDRE #######################
    ###############################################################################
    generate_set_Parent_Coupling([num_branches_to_firstStage, num_branches_to_secondStage, num_branches_to_thirdStage, num_branches_to_fourthStage, num_branches_to_fifthStage, num_branches_to_sixthStage, num_branches_to_seventhStage, num_branches_to_eighthStage, num_branches_to_ninthStage, num_branches_to_tenthStage, num_branches_to_eleventhStage, num_branches_to_twelfthStage, num_branches_to_thirteenthStage, num_branches_to_fourteenthStage, num_branches_to_fifteenthStage])

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


    def generate_node_probability(branch_counts):
        node_prob = {}
        current_node = 1

        # Process stage 1:
        stage1_nodes = branch_counts[0]
        if stage1_nodes == 0:
            raise ValueError("The first stage must have at least one node.")
        prob = 1.0 / stage1_nodes
        for _ in range(stage1_nodes):
            node_prob[current_node] = prob
            current_node += 1

        cumulative = stage1_nodes
        # For each subsequent stage:
        for i in range(1, len(branch_counts)):
            if branch_counts[i] == 0:
                # Skip this stage if branch count is zero.
                continue
            stage_nodes = cumulative * branch_counts[i]  # number of nodes in this stage
            prob = 1.0 / stage_nodes
            for _ in range(stage_nodes):
                node_prob[current_node] = prob
                current_node += 1
            cumulative *= branch_counts[i]
        return node_prob




    NodeProbability = generate_node_probability([num_branches_to_firstStage, num_branches_to_secondStage, num_branches_to_thirdStage, num_branches_to_fourthStage, num_branches_to_fifthStage, num_branches_to_sixthStage, num_branches_to_seventhStage, num_branches_to_eighthStage, num_branches_to_ninthStage, num_branches_to_tenthStage, num_branches_to_eleventhStage, num_branches_to_twelfthStage, num_branches_to_thirteenthStage, num_branches_to_fourteenthStage, num_branches_to_fifteenthStage])

    ###############################################################################
    # ----------------- Parameter data  -----------------
    ###############################################################################

    # --------------------------
    # Dictionaries
    # --------------------------
    if instance == 1: #Expected value
        if year == 2025:
            CostExpansion_Tec = {
                "Power_Grid": 1_000_000,
                "ElectricBoiler": 27.95,
                "HP_LT": 250.126,
                "HP_MT": 299.647,
                "PV": 213.391,
                "P2G": 222.977,
                "G2P": 719.551,
                "GasBoiler": 19.289,
                "GasBoiler_CCS": 230.209,
                "CHP": 277.527,
                "CHP_CCS": 488.447,
                "Biogas_Grid": 1_000_000,
                "CH4_Grid": 1_000_000,
                "CH4_H2_Mixer": 0,
                "DieselReserveGenerator": 125.536,
                "H2_Grid": 1_000_000,
                "Dummy_Grid": 1_000_000
            }
            

            CostExpansion_Bat = {
                "BESS_Li_Ion_1": 270.257,
                "BESS_Redox_1": 154.252,
                "CAES_1": 224.152,
                "Flywheel_1": 128.768,
                "Hot_Water_Tank_LT_1": 0.797,
                "H2_Storage_1": 15.124,
                "CH4_Storage_1": 0.03239
            }
        elif year == 2050:
            CostExpansion_Tec = {
                "Power_Grid": 1_000_000,
                "ElectricBoiler": 25.92968,
                "HP_LT": 212.06173,
                "HP_MT": 254.07956,
                "PV": 126.85121,
                "P2G": 94.45389,
                "G2P": 479.70075,
                "GasBoiler": 17.33577,
                "GasBoiler_CCS": 222.95651,
                "CHP": 270.55360,
                "CHP_CCS": 476.17433,
                "Biogas_Grid": 1_000_000,
                "CH4_Grid": 1_000_000,
                "CH4_H2_Mixer": 0,
                "DieselReserveGenerator": 123.99813,
                "H2_Grid": 1_000_000,
                "Dummy_Grid": 1_000_000
            }
            
            CostExpansion_Bat = {
                "BESS_Li_Ion_1": 86.10438,
                "BESS_Redox_1": 106.90110,
                "CAES_1": 217.38136,
                "Flywheel_1": 128.76774,
                "Hot_Water_Tank_LT_1": 0.79737,
                "H2_Storage_1": 6.23917,
                "CH4_Storage_1": 0.03239
            }

        else:
            raise ValueError("Invalid year. Please choose either 2025 or 2050.")

    elif instance == 2 or instance == 5: #Lowerbound
        if year == 2025:
            CostExpansion_Tec = {
                "Power_Grid": 1_000_000,
                "ElectricBoiler": 22.36,
                "HP_LT": 200.1008,
                "HP_MT": 239.7176,
                "PV": 170.7128,
                "P2G": 178.3816,
                "G2P": 575.6408,
                "GasBoiler": 15.4312,
                "GasBoiler_CCS": 184.1672,
                "CHP": 222.0216,
                "CHP_CCS": 390.7576,
                "Biogas_Grid": 1_000_000,
                "CH4_Grid": 1_000_000,
                "CH4_H2_Mixer": 0,
                "DieselReserveGenerator": 100.4288,
                "H2_Grid": 1_000_000,
                "Dummy_Grid": 1_000_000
            }

            CostExpansion_Bat = {
                "BESS_Li_Ion_1": 216.2056,
                "BESS_Redox_1": 123.4016,
                "CAES_1": 179.3216,
                "Flywheel_1": 103.0144,
                "Hot_Water_Tank_LT_1": 0.6376,
                "H2_Storage_1": 12.0992,
                "CH4_Storage_1": 0.025912
            }

        elif year == 2050:
            CostExpansion_Tec = {
                "Power_Grid": 1_000_000,
                "ElectricBoiler": 20.74374,
                "HP_LT": 169.64938,
                "HP_MT": 203.26365,
                "PV": 101.48097,
                "P2G": 75.56311,
                "G2P": 383.7606,
                "GasBoiler": 13.86862,
                "GasBoiler_CCS": 178.36521,
                "CHP": 216.44288,
                "CHP_CCS": 380.93946,
                "Biogas_Grid": 1_000_000,
                "CH4_Grid": 1_000_000,
                "CH4_H2_Mixer": 0,
                "DieselReserveGenerator": 99.198504,
                "H2_Grid": 1_000_000,
                "Dummy_Grid": 1_000_000
            }

            CostExpansion_Bat = {
                "BESS_Li_Ion_1": 68.883504,
                "BESS_Redox_1": 85.52088,
                "CAES_1": 173.905088,
                "Flywheel_1": 103.014192,
                "Hot_Water_Tank_LT_1": 0.637896,
                "H2_Storage_1": 4.991336,
                "CH4_Storage_1": 0.025912
            }


        else:
            raise ValueError("Invalid year. Please choose either 2025 or 2050.")
        
    elif instance == 3 or instance == 4: #Upperbound
        if year == 2025:
            CostExpansion_Tec = {
                "Power_Grid": 1_000_000,
                "ElectricBoiler": 33.54,
                "HP_LT": 300.1512,
                "HP_MT": 359.5764,
                "PV": 256.0692,
                "P2G": 267.5724,
                "G2P": 863.4612,
                "GasBoiler": 23.1468,
                "GasBoiler_CCS": 276.2508,
                "CHP": 333.0324,
                "CHP_CCS": 586.1364,
                "Biogas_Grid": 1_000_000,
                "CH4_Grid": 1_000_000,
                "CH4_H2_Mixer": 0,
                "DieselReserveGenerator": 150.6432,
                "H2_Grid": 1_000_000,
                "Dummy_Grid": 1_000_000
            }

            CostExpansion_Bat = {
                "BESS_Li_Ion_1": 324.3084,
                "BESS_Redox_1": 185.1024,
                "CAES_1": 268.9824,
                "Flywheel_1": 154.5216,
                "Hot_Water_Tank_LT_1": 0.9564,
                "H2_Storage_1": 18.1488,
                "CH4_Storage_1": 0.038868
            }

        elif year == 2050:
            CostExpansion_Tec = {
                "Power_Grid": 1_000_000,
                "ElectricBoiler": 31.115616,
                "HP_LT": 254.474076,
                "HP_MT": 304.895472,
                "PV": 152.221452,
                "P2G": 113.344668,
                "G2P": 575.6409,
                "GasBoiler": 20.802924,
                "GasBoiler_CCS": 267.547812,
                "CHP": 324.66432,
                "CHP_CCS": 571.409196,
                "Biogas_Grid": 1_000_000,
                "CH4_Grid": 1_000_000,
                "CH4_H2_Mixer": 0,
                "DieselReserveGenerator": 148.797756,
                "H2_Grid": 1_000_000,
                "Dummy_Grid": 1_000_000
            }

            CostExpansion_Bat = {
                "BESS_Li_Ion_1": 103.325256,
                "BESS_Redox_1": 128.28132,
                "CAES_1": 260.857632,
                "Flywheel_1": 154.521288,
                "Hot_Water_Tank_LT_1": 0.956844,
                "H2_Storage_1": 7.487004,
                "CH4_Storage_1": 0.038868
            }



        else:
            raise ValueError("Invalid year. Please choose either 2025 or 2050.")
    else:
        raise ValueError("Invalid instance number.")
    # --------------------------    --------------------------
    # --------------------------    --------------------------      

    CostGridTariff = 123.93




    ####################################################################################
    ########################### GET CHILD MAPPINNG FUNC #################################
    #####################################################################################

    def map_children_to_parents_from_file(tab_filename):
        # Les tab-fila (antatt tab-separert)
        df = pd.read_csv(tab_filename, sep="\t")
        
        # Bygg et dictionary med umiddelbare relasjoner: barn -> forelder
        child_to_parent = {row["Node"]: row["Parent"] for _, row in df.iterrows()}
        
        # Funksjon for å finne top-level forelder ved å følge oppover i treet
        def find_top(node):
            # Hvis node ikke finnes som barn (nøkkel) i child_to_parent,
            # er den top-level (det antas at top-level foreldre kun er i Parent-kolonnen)
            if node not in child_to_parent:
                return node
            else:
                return find_top(child_to_parent[node])
        
        # Beregn top-level for alle noder (som finnes som barn)
        top_level = {}
        for node in child_to_parent:
            top_level[node] = find_top(node)
        
        # Gruppér noder etter top-level forelder
        grouping = {}
        for node, top in top_level.items():
            grouping.setdefault(top, []).append(node)
        
        return grouping

    def extract_parent_coupling(tab_filename = "Set_ParentCoupling.tab"):
        df = pd.read_csv(tab_filename, sep="\t")
        data = {
            "Node": df["Node"].tolist(),
            "Parent": df["Parent"].tolist()
        }
        return data

    data = extract_parent_coupling()
    df_example = pd.DataFrame(data)
    taB_filenam = "Set_ParentCoupling.tab"
    df_example.to_csv(taB_filenam, sep = "\t", index=False, header=True, lineterminator='\n')
    mapping = map_children_to_parents_from_file(taB_filenam)
    print("Førstestegs-forelder : -> [alle etterkommere]:")
    mapping_converted = {int(k): [int(x) for x in v] for k, v in mapping.items()}
    print(mapping_converted)

    ####################################################################################
    ########################### GET PARENT MAPPING FUNC #################################
    #####################################################################################
    def create_parent_mapping(filepath):
        """
        Leser en .tab-fil med kolonnene 'Node' og 'Parent',
        og returnerer en parent_mapping som et Python-dictionary.
        """
        # Les filen
        df = pd.read_csv(filepath, sep="\t")
        
        # Sjekk at nødvendige kolonner finnes
        if not {"Node", "Parent"}.issubset(df.columns):
            raise ValueError("Filen må inneholde kolonnene 'Node' og 'Parent'.")

        # Lag parent_mapping
        parent_mapping = dict(zip(df["Node"], df["Parent"]))
        
        return parent_mapping


    # ----------------- HISTORICAL PRICE DATA HANDLING -----------------

    # Load data
    df = pd.read_excel(excel_path, sheet_name="2024 NO1 data")

    # Group by full (month, day) sets
    df_grouped = df.groupby(["Month", "Day"])
    day_data_map = {
        (month, day): group.reset_index(drop=True)
        for (month, day), group in df_grouped
        if len(group) == 24
    }

    # ---- Replace the manual node_month_ranges with parent-group-based logic ----
    #####################################################################################
    ########################### CLUSTER BASERT PÅ 4 ÅRSTIDER ############################
    #####################################################################################
    """
    parent_month_mapping = {
        1: [12, 1, 2],
        2: [3, 4, 5],
        3: [6, 7, 8],
        4: [9, 10, 11],
    }
    """
    #####################################################################################
    ########################### CLUSTER BASERT PÅ 2 ÅRSTIDER ############################
    #####################################################################################
    parent_month_mapping = {
        1: [4, 5, 6, 7, 8, 9],
        2: [1, 2, 3, 10, 11, 12],
    }

    # Extend parent_month_mapping assignment to parent nodes as well
    node_to_day = {}

    # For grupper med foreldre (vanlige tilfeller)
    for parent, child_nodes in mapping_converted.items():
        allowed_months = parent_month_mapping.get(parent, [1, 2, 3])
        valid_days = [d for d in day_data_map if d[0] in allowed_months]

        if not valid_days:
            raise ValueError(f"No valid historical days for months {allowed_months} in parent group {parent}")

        all_nodes = [parent] + child_nodes
        for node in all_nodes:
            node_to_day[node] = random.choice(valid_days)

    # ➕ Legg til førstestegsnoder manuelt basert på parent_month_mapping
    for node in range(1, num_firstStageNodes + 1):
        allowed_months = parent_month_mapping.get(node, [1, 2, 3])
        valid_days = [d for d in day_data_map if d[0] in allowed_months]
        
        if not valid_days:
            raise ValueError(f"No valid historical days for months {allowed_months} for first-stage node {node}")
        
        node_to_day[node] = random.choice(valid_days)

    # Print assignment (month and day) for traceability
    print("\n📅 Random day selected for each node:")
    for node in sorted(node_to_day):
        m, d = node_to_day[node]
        print(f"Node {node}: Month = {m:02d}, Day = {d:02d}")


    # Extract dictionary for reference demand and prices 

    def extract_series_for_column(columns, node_to_day, day_data_map, all_keys=None, fill_zero_for_missing=True):
        """
        Extracts 24-hour time series data for specified columns across nodes.

        Parameters:
        - columns: list of column names in the Excel file to extract
        - node_to_day: mapping of node -> (month, day)
        - day_data_map: mapping of (month, day) -> DataFrame with hourly data
        - all_keys: list of all expected keys (e.g., all fuels or all price types)
        - fill_zero_for_missing: if True, fill missing keys with zero time series

        Returns:
        - Dictionary: {key: {node: {timestep: value}}}
        """
        result = {}

        for col in columns:
            result[col] = {}
            for node, (month, day) in node_to_day.items():
                df_day = day_data_map[(month, day)]
                result[col][node] = {t + 1: float(df_day[col].iloc[t]) for t in range(24)}

        if fill_zero_for_missing and all_keys:
            for key in all_keys:
                if key not in result:
                    result[key] = {node: {t: 0.0 for t in range(1, 25)} for node in node_to_day}

        return result

    # ✅ Define demand-related inputs
    demand_columns = ["Electricity", "LT", "MT", "CH4"]
    all_fuels = ["Electricity", "LT", "MT", "H2", "CH4", "Biogas", "CH4_H2_Mix"]

    # ✅ Build ReferenceDemand using the unified extractor
    ReferenceDemand = extract_series_for_column(
        columns=demand_columns,
        node_to_day=node_to_day,
        day_data_map=day_data_map,
        all_keys=all_fuels,
        fill_zero_for_missing=True
    )

    # ✅ Define and extract price-related dictionaries
    SpotPrice = extract_series_for_column(["Day-ahead Price (EUR/MWh)"], node_to_day, day_data_map)["Day-ahead Price (EUR/MWh)"]
    IntradayPrice = extract_series_for_column(["Intraday price (EUR/MWh)"], node_to_day, day_data_map)["Intraday price (EUR/MWh)"]
    ActivationUpPrice = extract_series_for_column(["Activation price up (mFRR)"], node_to_day, day_data_map)["Activation price up (mFRR)"]
    ActivationDwnPrice = extract_series_for_column(["Activation price down (mFRR)"], node_to_day, day_data_map)["Activation price down (mFRR)"]
    CapacityUpPrice = extract_series_for_column(["Capacity price up (mFRR)"], node_to_day, day_data_map)["Capacity price up (mFRR)"]
    CapacityDwnPrice = extract_series_for_column(["Capacity price down (mFRR)"], node_to_day, day_data_map)["Capacity price down (mFRR)"]
    PV_data = extract_series_for_column(["Soldata"], node_to_day, day_data_map)["Soldata"]
    Res_CapacityUpVolume = extract_series_for_column(["Res_Cap_Volume_Up"], node_to_day, day_data_map)["Res_Cap_Volume_Up"]
    Res_CapacityDwnVolume = extract_series_for_column(["Res_Cap_Volume_Down"], node_to_day, day_data_map)["Res_Cap_Volume_Down"]
    ID_Capacity_Sell_Volume = extract_series_for_column(["ID_Cap_Volume_Sell"], node_to_day, day_data_map)["ID_Cap_Volume_Sell"]
    ID_Capacity_Buy_Volume = extract_series_for_column(["ID_Cap_Volume_Buy"], node_to_day, day_data_map)["ID_Cap_Volume_Buy"]






    #Create Tech_availability:

    Tech_availability = {
        "PV": PV_data,
        "Power_Grid": 1.0,
        "ElectricBoiler": 0.98,
        "HP_LT": 0.98,
        "HP_MT": 0.98,
        "P2G": 0.98,
        "G2P": 0.98,
        "GasBoiler": 0.98,
        "GasBoiler_CCS": 0.98,
        "CHP": 0.8,
        "CHP_CCS": 0.8,
        "Biogas_Grid": 0.9,
        "CH4_Grid": 0.8,
        "CH4_H2_Mixer": 1.0,
        "DieselReserve_Generator": 0.98,
        "H2_Grid": 0.8,
        "Dummy_Grid": 1.0,
    }



    import pprint

    def average_dict_values(nested_dict):
        total = 0
        count = 0
        for node_data in nested_dict.values():
            for hour_value in node_data.values():
                total += hour_value
                count += 1
        return total / count if count > 0 else 0

    avg_capacity_up = average_dict_values(Res_CapacityUpVolume)
    avg_capacity_down = average_dict_values(Res_CapacityDwnVolume)

    print(f"Average Capacity Up Price: {avg_capacity_up:.2f} EUR/MW")
    print(f"Average Capacity Down Price: {avg_capacity_down:.2f} EUR/MW")

    # Preview one node per fuel
    pprint.pprint({k: list(v.items())[:1] for k, v in ReferenceDemand.items()})
    pprint.pprint({k: list(v.items())[:1] for k, v in Res_CapacityDwnVolume.items()})
    pprint.pprint({k: list(v.items())[:1] for k, v in ID_Capacity_Buy_Volume.items()})


    parent_mapping = create_parent_mapping("Set_ParentCoupling.tab")

    

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

            if excel_path == "NO1_Aluminum_2024_combined historical data.xlsx":
                # Use all periods
                df_all = pd.DataFrame({"LoadShiftingPeriod": df_periods["Periods"]})
                yield df_all
            elif excel_path == "NO1_Pulp_Paper_2024_combined historical data.xlsx" or excel_path == "NO1_Pulp_Paper_2024_combined historical data_Uten_SatSun.xlsx":
                # Use only the last period
                last_period = df_periods["Periods"].max()
                df_last = pd.DataFrame({"LoadShiftingPeriod": [last_period]})
                yield df_last
            else:
                raise ValueError(f"Unknown excel_path: {excel_path}")

        # Use the make_tab_file function to write the result
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


  

    ##########
    ## Additional Parameter generation functions ###
    ##########
    def generate_cost_activity(num_nodes, num_timesteps, cost_activity, filename="Par_ActivityCost.tab"):
        def data_generator(chunk_size=10_000_000):
            rows = []
            count = 0
            for node in range(num_firstStageNodes + 1, num_nodes + 1):
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

    def generate_activation_factors_with_rest_time(num_nodes, num_timesteps, parent_mapping, activation_rate=0.10, rest_hours=8):
    
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
    
    def generate_max_upshift_file(excel_path, num_timesteps, filename="Par_MaxUpShift.tab"):
        shift_hours = range(8, 18)

        if "Pulp_Paper" in excel_path:
            factor = 0.1
            industry = "pulp"
        elif "Aluminum" in excel_path:
            factor = 0.05
            industry = "alu"
        else:
            raise ValueError("Invalid excel_path: must contain 'Pulp_Paper' or 'Aluminum'")

        
        with open(filename, "w", encoding="utf-8") as f:
            f.write("Time\tMaximumUpShift\n")
            for t in range(1, num_timesteps + 1):
                if industry == "alu":
                    value = factor
                elif industry == "pulp":
                    value = factor if t in shift_hours else 0
                f.write(f"{t}\t{value}\n")


    def generate_max_downshift_file(excel_path, num_timesteps, filename="Par_MaxDwnShift.tab"):
        shift_hours = range(8, 18)

        if "Pulp_Paper" in excel_path:
            factor = 0.3
            industry = "pulp"
        elif "Aluminum" in excel_path:
            factor = 0.2
            industry = "alu"
        else:
            raise ValueError("Invalid excel_path: must contain 'Pulp_Paper' or 'Aluminum'")

        
        with open(filename, "w", encoding="utf-8") as f:
            f.write("Time\tMaximumDwnShift\n")
            for t in range(1, num_timesteps + 1):
                if industry == "alu":
                    value = factor
                elif industry == "pulp":
                    value = factor if t in shift_hours else 0
                f.write(f"{t}\t{value}\n")

    


    def generate_joint_regulation_activation_files(num_nodes, num_timesteps, up_filename = "Par_ActivationFactor_Up_Reg.tab", down_filename = "Par_ActivationFactor_Dwn_Reg.tab"):
        if excel_path == "NO1_Pulp_Paper_2024_combined historical data.xlsx" or excel_path == "NO1_Pulp_Paper_2024_combined historical data_Uten_SatSun.xlsx":
            df_joint = generate_activation_factors(num_nodes, num_timesteps, parent_mapping)
        elif excel_path == "NO1_Aluminum_2024_combined historical data.xlsx":
            df_joint = generate_activation_factors(num_nodes, num_timesteps, parent_mapping)
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
    generate_set_of_NodesInStage([num_branches_to_firstStage, num_branches_to_secondStage, num_branches_to_thirdStage, num_branches_to_fourthStage, num_branches_to_fifthStage, num_branches_to_sixthStage, num_branches_to_seventhStage, num_branches_to_eighthStage, num_branches_to_ninthStage, num_branches_to_tenthStage, num_branches_to_eleventhStage, num_branches_to_twelfthStage, num_branches_to_thirteenthStage, num_branches_to_fourteenthStage, num_branches_to_fifteenthStage])
    generate_set_of_Periods([num_branches_to_firstStage, num_branches_to_secondStage, num_branches_to_thirdStage, num_branches_to_fourthStage, num_branches_to_fifthStage, num_branches_to_sixthStage, num_branches_to_seventhStage, num_branches_to_eighthStage, num_branches_to_ninthStage, num_branches_to_tenthStage, num_branches_to_eleventhStage, num_branches_to_twelfthStage, num_branches_to_thirteenthStage, num_branches_to_fourteenthStage, num_branches_to_fifteenthStage])
    generate_set_of_PeriodsInMonth([num_branches_to_firstStage, num_branches_to_secondStage, num_branches_to_thirdStage, num_branches_to_fourthStage, num_branches_to_fifthStage, num_branches_to_sixthStage, num_branches_to_seventhStage, num_branches_to_eighthStage, num_branches_to_ninthStage, num_branches_to_tenthStage, num_branches_to_eleventhStage, num_branches_to_twelfthStage, num_branches_to_thirteenthStage, num_branches_to_fourteenthStage, num_branches_to_fifteenthStage])
    generate_set_of_LoadShiftingPeriod()
    generate_set_of_NodesFirst(num_branches_to_firstStage)

    ##########################################################################
    ########################### GENERATE PARAMETERS ##########################
    ##########################################################################

    generate_cost_activity(num_nodes, num_timesteps, cost_activity)
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
    generate_max_upshift_file(excel_path, num_timesteps=24)
    generate_max_downshift_file(excel_path, num_timesteps=24)


  # Call them after Set_of_Periods.tab is created
    generate_Par_CostExpansion_Tec()
    generate_Par_CostExpansion_Bat()
    generate_Par_CostGridTariff()
    generate_Par_LastPeriodInMonth()