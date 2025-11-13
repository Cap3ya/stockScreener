from lib.logger import Logger
from lib.analysis_orchestrator import AnalysisOrchestrator

def main():
    logger = Logger.setup()
    orchestrator = AnalysisOrchestrator(logger)
    orchestrator.run()

if __name__ == "__main__":
    main()
