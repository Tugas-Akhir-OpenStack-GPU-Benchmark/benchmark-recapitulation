import os

from pretty_html_table import build_table
import pandas as pd
import seaborn as sns
from google.oauth2 import service_account

from matplotlib import pyplot as plt
from pandas.plotting import table

from ResultProcessors import ResultProcessors
from aesthetic_pandas_export import export_pandas_to_png
from constants import openstack_service_col, group_col, zun_const, ironic_const, physical_machine_const, nova_const, \
    value_col, stat_name_col
from glmark2_extractor import MultiresolutionGlmark2ResultProcessor
from namd_extractor import NamdResultProcessor
from pytorch_extractor import PytorchResultProcessor
from stats_recap import StatRecapPerOpenStackService


class UpdateGraphics():
    def __init__(self, openstack_services_stat_recap: dict[str, StatRecapPerOpenStackService],
                 glmark2_processors: dict[str, MultiresolutionGlmark2ResultProcessor],
                 namd_processors:dict[str, NamdResultProcessor], pytorch_processors: dict[str, PytorchResultProcessor]):
        self.openstack_services_stat_recap = openstack_services_stat_recap
        self.glmark2_processors = glmark2_processors
        self.namd_processors = namd_processors
        self.pytorch_processors = pytorch_processors
        os.makedirs("./graphics", exist_ok=True)

    def update_slides(self):
        self.stat_recap_pd = StatRecapPerOpenStackService.as_dataframe(self.openstack_services_stat_recap)
        self.stat_recap_pd[openstack_service_col] = self.stat_recap_pd[openstack_service_col].apply(convert_to_openstack_name)
        self.update_slides_glmark2()
        self.update_slides_namd()
        self.update_slides_pytorch()

    def do_graphic(self, curr_data, y_col, title, save_file, group, benchmark):
        plt.figure()
        boxplot = sns.boxplot(data=curr_data, x=openstack_service_col, y=y_col).set_title(title)
        figure = boxplot.get_figure()
        figure.savefig(f"./graphics/boxplot_{save_file}.png", transparent=True)

        fig, ax = plt.subplots(1, 1)
        sns.kdeplot(data=curr_data, hue=openstack_service_col, x=y_col, ax=ax, bw_adjust=1.8)
        ax.set_title(title)
        ax.set_ylabel(ylabel="")
        fig.savefig(f"./graphics/bellcurve_{save_file}.png", transparent=True)

        dataframe = self.stat_recap_pd[(self.stat_recap_pd[group_col] == group)]
        dataframe = dataframe[dataframe['benchmark'] == benchmark]
        original_order = dataframe[stat_name_col].unique()  # to maintain original order of stat_name_col column
        dataframe = dataframe.pivot(columns=openstack_service_col, index=stat_name_col, values=value_col).reindex(
            index=original_order)
        dataframe = dataframe[[physical_machine_const, nova_const, zun_const, ironic_const]]
        dataframe = dataframe.reset_index(stat_name_col)

        export_pandas_to_png(dataframe, f"./graphics/table_{save_file}.png", title=title, hide_index=True)


    def update_slides_glmark2(self):
        data = dataframe_from_dict_of_processor(self.glmark2_processors)
        data[openstack_service_col] = data[openstack_service_col].apply(convert_to_openstack_name)

        for resolution in data['resolution'].unique():
            curr_data = data[data['resolution'] == resolution]
            self.do_graphic(curr_data, "FPS", f'Glmark2 {resolution}', f"glmark2_{resolution}", resolution, "Glmark2")

    def update_slides_pytorch(self):
        data = dataframe_from_dict_of_processor(self.pytorch_processors)
        data[openstack_service_col] = data[openstack_service_col].apply(convert_to_openstack_name)

        for model in data['model'].unique():
            curr_data = data[data['model'] == model]
            self.do_graphic(curr_data, "batches/second",
                            f'Phoronix PyTorch {model}', f"pytorch_{model}", model, "PyTorch")

    def update_slides_namd(self):
        data = dataframe_from_dict_of_processor(self.namd_processors)
        data[openstack_service_col] = data[openstack_service_col].apply(convert_to_openstack_name)
        self.do_graphic(data, "days/ns", f'Phoronix NAMD', f"boxplot_namd", "", "NAMD")


def convert_to_openstack_name(file_name):
    file_name = file_name.lower()
    if 'nova' in file_name:
        return nova_const
    if 'zun' in file_name:
        return zun_const
    if 'ironic' in file_name:
        return ironic_const
    if 'physical' in file_name:
        return physical_machine_const
    assert False


def abc(df, group_by, to_be_replicated):
    temporary_col = f"{group_by}x"
    df[temporary_col] = 1
    df[temporary_col] = df.groupby("ride_ID")[temporary_col].transform("cumsum").apply(lambda x: f"person_ID{x}")

    df = df.pivot(index="ride_ID", columns="person_IDx", values="person_ID").reset_index().rename_axis(columns={"person_IDx":""})




def dataframe_from_dict_of_processor(dictionary: dict[str, ResultProcessors]):
    ret = []
    for openstack_service, resolutions in dictionary.items():
        dataframe = resolutions.as_dataframe()
        dataframe[openstack_service_col] = openstack_service
        ret.append(dataframe)
    return pd.concat(ret)
