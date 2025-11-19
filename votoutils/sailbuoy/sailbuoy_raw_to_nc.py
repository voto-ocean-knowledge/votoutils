from pathlib import Path
import logging
import yaml
_log = logging.getLogger(__name__)

from votoutils.sailbuoy.sailbuoy_functions import parse_nbosi, \
    parse_airmar, parse_data, parse_nrt, parse_legato, parse_gmx560, parse_mose, \
    export_netcdf, merge_intermediate, parse_aanderaa_ctd, parse_aanderaa_adcp

sensor_to_dir = {
    'RBR legato CTD': 'LEGATO',
    'Neil Brown 100': 'NBOSI',
    'Aanderaa 4319A': 'AADICOND',
    'Aanderaa DCPS 5400': 'DCPS',
    'Gill Instruments GMX560': 'MAXIMET',
    'Airmar 200WX': 'AIRMAR',
    'Datawell MOSE-G1000': 'MOSE',
    'Sailbuoy autopilot': 'Auto_pilot',
    'Sailbuoy datalogger': 'Data_logger'
}

sensor_to_function = {
    'RBR legato CTD': parse_legato,
    'Neil Brown 100': parse_nbosi,
    'Aanderaa 4319A': parse_aanderaa_ctd,
    'Aanderaa DCPS 5400': parse_aanderaa_adcp,
    'Gill Instruments GMX560': parse_gmx560,
    'Airmar 200WX': parse_airmar,
    'Datawell MOSE-G1000': parse_mose,
    'Sailbuoy autopilot': parse_nrt,
    'Sailbuoy datalogger': parse_data
}

class Sailbuoy:
    def __init__(self, input_dir=".", base_dir=".", yaml_path="."):
        self.input_dir = Path(input_dir)
        self.base_dir = Path(base_dir)
        self.yaml_path = yaml_path
        self.output_dir = self.base_dir
        self.intermediate_dir = self.output_dir / "intermediate_data"
        self.nmea_dir = self.output_dir / "NMEA"
        self.mose_dir = self.output_dir / "MOSE"
        self.reprocess_raw = False

        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True)
        if not self.intermediate_dir.exists():
            self.intermediate_dir.mkdir(parents=True)
        with open(self.yaml_path) as fin:
            self.config = yaml.safe_load(fin)
        self.platform_serial =  self.config['metadata']['platform_serial']
        expected_sensors = list(sensor['make_model'] for sensor in self.config['devices'].values())
        expected_dirs = {sensor_to_dir[sensor] for sensor in expected_sensors}
        input_dirs = {path.name for path in self.input_dir.glob('*')}
        ignore_dirs = {'Metadata', 'Track'}
        process_dirs = input_dirs - ignore_dirs
        if not process_dirs == expected_dirs:
            _log.error(f"Expected dirs {expected_dirs}, found {process_dirs} in {self.input_dir}")
        self.sensors_to_process = expected_sensors
        _log.info(f"Will process data from {expected_sensors}")

    def parse_sensors(self):
        for sensor in self.sensors_to_process:
            proc_function = sensor_to_function[sensor]
            sensor_dir = sensor_to_dir[sensor]
            if not (self.input_dir / sensor_dir).exists():
                _log.error(f"Input sensor dir {sensor_dir} not found")
                continue
            outfile = self.intermediate_dir / f"{sensor_dir}.pqt"
            if outfile.exists() and not self.reprocess_raw:
                _log.info(f"{sensor}: {sensor_dir} already processed, skipping")
                continue
            _log.info(f"Process {sensor}")
            proc_function(self.input_dir / sensor_dir, self.intermediate_dir)
        _log.info("Completed sensor parse")

    def merge_intermediate(self):
        merge_intermediate(self.intermediate_dir, self.output_dir)

    def export_netcdf(self):
        export_netcdf(self.output_dir, self.config)

    def process(self):
        self.parse_sensors()
        self.merge_intermediate()
        self.export_netcdf()

if __name__ == "__main__":
    logging.basicConfig(
        filename="/data/log/sailbuoy_delayed.log",
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    id=2017
    if id==2017:
        sb = Sailbuoy(
            "/mnt/samba/43_Hudson Bay Sailbuoy 2/3_Non_Processed/SB2017/SB2017_M7",
            "/home/callum/Downloads/tmpsb/SB2017/M7",
            "/home/callum/Documents/data-flow/raw-to-nc/deployment-yaml/sailbuoy_yaml/SB2017_M7.yml"
        )
    elif id==2121:
        sb = Sailbuoy(
            "/mnt/samba/45_SkaMix/3_Non_Processed/SB2121/SB2121_M2",
            "/home/callum/Downloads/tmpsb/SB2121/M2",
            "/home/callum/Documents/data-flow/raw-to-nc/deployment-yaml/sailbuoy_yaml/SB2121_M2.yml"
        )
        sb.platform_serial = "SB2121"
    else:
        sb = Sailbuoy(
            "/mnt/samba/35_Windwake/3_Non_Processed/SB2120/SB2120_M3",
            "/home/callum/Downloads/tmpsb/SB2120/M3",
                    "/home/callum/Documents/data-flow/raw-to-nc/deployment-yaml/sailbuoy_yaml/SB2120_M3.yml"
        )
    sb.parse_sensors()