# main_cleanup_orchestrator.py
#
# This script orchestrates the entire semantic layer cleanup process by running
# the four main scripts in sequence. It provides a single entry point for the
# entire workflow and asks for user confirmation before proceeding with each major step.
#
# To Run:
# 1. Make sure you have configured your `.env` file correctly.
# 2. Execute this script from your terminal: `python run_cleanup_pipeline.py`

import sys
import subprocess
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# The validation command is now configurable via the .env file
INITIAL_VALIDATION_COMMAND = os.getenv('INITIAL_VALIDATION_COMMAND')

def ask_to_proceed(step_name, description):
    """Generic function to ask for user confirmation."""
    print("\n" + "="*80)
    print(f"Next Step: {step_name}")
    print(description)
    print("="*80)
    user_input = input("Do you want to proceed? (yes/no): ").lower()
    if user_input in ['yes', 'y']:
        print("✅ Proceeding...")
        return True
    else:
        print("⏭️ Skipping step...")
        return False


def main():
    """Main orchestrator function."""
    print("\n" + "="*80)
    print("⚠️ Welcome to the Semantic Layer Cleanup Pipeline! ⚠️")
    print("This script will guide you through identifying and commenting out unused fields.")
    print("You can choose to skip analytical steps if you have already generated the output files.")
    print("="*80)

    # --- Step 0: Initial Validation ---
    if ask_to_proceed(
        "Step 0: Validate Lightdash Project",
        "This will run 'lightdash validate' to check for any existing issues before starting.\nIt is recommended, but can be skipped if you are confident your project is in a good state."
    ):
    
        if not INITIAL_VALIDATION_COMMAND:
            print("🚨 ERROR: INITIAL_VALIDATION_COMMAND is not set in your .env file.")
            print("Please configure it to run Lightdash validation (e.g., conda run -n dbt lightdash validate...). Exiting.")
            sys.exit(1)

        try:
            # Determine the DBT_DIR for running the validation command
            # Assuming DBT_DIR is the parent of the semantic_layer_cleanup directory
            dbt_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            
            print(f"  -> Running validation command: {INITIAL_VALIDATION_COMMAND} in directory {dbt_dir}")
            subprocess.run(
                INITIAL_VALIDATION_COMMAND,
                shell=True,
                cwd=dbt_dir,
                check=True,
                text=True
            )
            print("✅ Step 0: Lightdash validation completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"\n🚨 CRITICAL ERROR: Lightdash validation failed in Step 0. This indicates an issue in your dbt project and/or semantic layer that MUST be fixed before proceeding with the cleanup pipeline.")
            print(f"Please review the validation output below and resolve the reported issues.")
            print("Lightdash Validate Output (stdout):")
            print(e.stdout)
            print("Lightdash Validate Output (stderr):")
            print(e.stderr)
            sys.exit(1)
        except FileNotFoundError:
            print("🚨 Error: Validation command not found. Make sure it's correctly configured in your .env.")
            sys.exit(1)
        except Exception as e:
            print(f"🚨 An unexpected error occurred during Step 0 (Lightdash validation): {e}")
            sys.exit(1)

    # --- Step 1 ---
    if ask_to_proceed(
        "Step 1: Analyse Semantic Layer Objects",
        "This will parse all local dbt YAML files to extract dimensions, metrics, and their dependencies."
    ):
        try:
            subprocess.run(
                [sys.executable, "1_semantic_layer_objects.py"],
                cwd=os.path.dirname(__file__),
                check=True,
                text=True
            )
            print("✅ Step 1 completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"🚨 An error occurred during Step 1: {e}")
            sys.exit(1)
        except FileNotFoundError:
            print("🚨 Error: '1_semantic_layer_objects.py' not found. Make sure it's in the same directory.")
            sys.exit(1)
        except Exception as e:
            print(f"🚨 An unexpected error occurred during Step 1: {e}")
            sys.exit(1)

    # --- Step 2 ---
    print("\n" + "="*80)
    print("Next Step: Step 2: Analyse Semantic Objects Used in Charts")
    print("This step will analyze chart usage. It will first ask to run 'lightdash download' and then ask to parse the local chart files. Both sub-steps can be skipped.")
    print("="*80)
    try:
        subprocess.run(
            [sys.executable, "2_semantic_objects_used_in_charts.py"],
            cwd=os.path.dirname(__file__),
            check=True,
            text=True
        )
        print("✅ Step 2 completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"🚨 An error occurred during Step 2: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("🚨 Error: '2_semantic_objects_used_in_charts.py' not found. Make sure it's in the same directory.")
        sys.exit(1)
    except Exception as e:
        print(f"🚨 An unexpected error occurred during Step 2: {e}")
        sys.exit(1)

    # --- Step 3 ---
    if ask_to_proceed(
        "Step 3: Consolidate Object Usage",
        "This step will correlate the outputs of the first two steps to determine which fields are used directly, indirectly (as a dependency), or not at all."
    ):
        try:
            subprocess.run(
                [sys.executable, "3_semantic_layer_objects_usage.py"],
                cwd=os.path.dirname(__file__),
                check=True,
                text=True
            )
            print("✅ Step 3 completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"🚨 An error occurred during Step 3: {e}")
            sys.exit(1)
        except FileNotFoundError:
            print("🚨 Error: '3_semantic_layer_objects_usage.py' not found. Make sure it's in the same directory.")
            sys.exit(1)
        except Exception as e:
            print(f"🚨 An unexpected error occurred during Step 3: {e}")
            sys.exit(1)

    # --- Step 4 ---
    print("\n" + "="*80)
    print("Final Step: Step 4: Perform Semantic Layer Cleanup ⚠️⚠️⚠️")
    print("This is the final step. It will read the usage analysis and begin commenting out unused fields from your dbt YAML files.")
    print("It will validate each change and revert any that cause errors.")
    print("You will be asked for a final confirmation before any files are modified.")
    try:
        subprocess.run(
            [sys.executable, "4_semantic_layer_cleanup.py"],
            cwd=os.path.dirname(__file__),
            check=True,
            text=True
        )
        print("✅ Step 4 completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"🚨 An error occurred during Step 4: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("🚨 Error: '4_semantic_layer_cleanup.py' not found. Make sure it's in the same directory.")
        sys.exit(1)
    except Exception as e:
        print(f"🚨 An unexpected error occurred during Step 4: {e}")
        sys.exit(1)
        
    print("\n" + "="*80)
    print("✅ Semantic layer cleanup pipeline finished successfully!")
    print("="*80)


if __name__ == '__main__':
    main()
