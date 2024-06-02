import os

import pandas as pd
import seaborn as sns
from google.oauth2 import service_account

from matplotlib import pyplot as plt

from ResultProcessors import ResultProcessors
from glmark2_extractor import MultiresolutionGlmark2ResultProcessor
from namd_extractor import NamdResultProcessor
from pytorch_extractor import PytorchResultProcessor


openstack_service_col = 'openstack-service'


class UpdateGraphics():
    def __init__(self, openstack_services, glmark2_processors: dict[str, MultiresolutionGlmark2ResultProcessor],
                 namd_processors:dict[str, NamdResultProcessor], pytorch_processors: dict[str, PytorchResultProcessor]):


        self.glmark2_processors = glmark2_processors
        self.namd_processors = namd_processors
        self.pytorch_processors = pytorch_processors
        os.makedirs("./graphics", exist_ok=True)



    def update_slides(self):
        self.update_slides_glmark2()
        self.update_slides_namd()
        self.update_slides_pytorch()

    def do_graphic(self, curr_data, y_col, title, save_file):
        plt.figure()
        boxplot = sns.boxplot(data=curr_data, x=openstack_service_col, y=y_col).set_title(title)
        figure = boxplot.get_figure()
        figure.savefig(f"./graphics/boxplot_{save_file}.png")

        fig, ax = plt.subplots(1, 1)
        sns.kdeplot(data=curr_data, hue=openstack_service_col, x=y_col, ax=ax, bw_adjust=1.8)
        ax.set_title(title)
        ax.set_ylabel(ylabel="")
        fig.savefig(f"./graphics/bellcurve_{save_file}.png")

    def update_slides_glmark2(self):
        data = dataframe_from_dict_of_processor(self.glmark2_processors)
        data[openstack_service_col] = data[openstack_service_col].apply(convert_to_openstack_name)

        for resolution in data['resolution'].unique():
            curr_data = data[data['resolution'] == resolution]
            self.do_graphic(curr_data, "FPS", f'Glmark2 {resolution}', f"glmark2_{resolution}")

    def update_slides_pytorch(self):
        data = dataframe_from_dict_of_processor(self.pytorch_processors)
        data[openstack_service_col] = data[openstack_service_col].apply(convert_to_openstack_name)

        for model in data['model'].unique():
            curr_data = data[data['model'] == model]
            self.do_graphic(curr_data, "batches/second",
                            f'Phoronix PyTorch {model}', f"pytorch_{model}")

    def update_slides_namd(self):
        data = dataframe_from_dict_of_processor(self.namd_processors)
        data[openstack_service_col] = data[openstack_service_col].apply(convert_to_openstack_name)
        self.do_graphic(data, "days/ns", f'Phoronix NAMD', f"boxplot_namd")


def convert_to_openstack_name(file_name):
    file_name = file_name.lower()
    if 'nova' in file_name:
        return "Nova"
    if 'zun' in file_name:
        return "Zun"
    if 'ironic' in file_name:
        return "Ironic"
    if 'physical' in file_name:
        return "Physical Machine"
    assert False


def dataframe_from_dict_of_processor(dictionary: dict[str, ResultProcessors]):
    ret = []
    for openstack_service, resolutions in dictionary.items():
        dataframe = resolutions.as_dataframe()
        dataframe[openstack_service_col] = openstack_service
        ret.append(dataframe)
    return pd.concat(ret)
