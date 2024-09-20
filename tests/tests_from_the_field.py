from pathlib import Path
from .shared import counter, read
import pypipegraph2 as ppg
import pytest


class DummyObject:
    pass


def dummy_smfg(files, prefix):
    Path(prefix).mkdir(exist_ok=True, parents=True)
    for f in files:
        f.write_text("hello")


def dummy_mfg(files):
    for f in files:
        f.parent.mkdir(exist_ok=True, parents=True)
        f.write_text("hello")


def dummy_fg(of):
    of.parent.mkdir(exist_ok=True, parents=True)
    of.write_text("fg")


def dummy_fg_raising(of):
    of.parent.mkdir(exist_ok=True, parents=True)
    raise ValueError()
    of.write_text("fg")


@pytest.mark.usefixtures("ppg2_per_test")
class TestsFromTheField:
    def test_issue_20210726a(self, job_trace_log):
        """This uncovered a depth first vs breadth first invalidation proagation bug.
        Created with Job_Status.dump_subgraph_for_debug and then heavily pruned
        """

        job_0 = ppg.FileGeneratingJob("J0", dummy_fg, depend_on_function=False)
        job_2 = ppg.DataLoadingJob("J2", lambda: None, depend_on_function=False)
        job_3 = ppg.DataLoadingJob("J3", lambda: None, depend_on_function=False)
        job_76 = ppg.FileGeneratingJob("J76", dummy_fg, depend_on_function=False)

        edges = []
        edges.append(("J0", "J2"))
        edges.append(("J2", "J3"))
        edges.append(("J2", "J76"))
        edges.append(("J76", "J3"))

        for a, b in edges:
            if a in ppg.global_pipegraph.jobs and b in ppg.global_pipegraph.jobs:
                ppg.global_pipegraph.jobs[a].depends_on(ppg.global_pipegraph.jobs[b])
            else:
                print("unused edge", a, b)

        ppg.run()
        ppg.run(event_timeout=1)

    def test_issue_20210726b(self, job_trace_log):
        job_0 = ppg.FileGeneratingJob("0", dummy_fg, depend_on_function=False)
        job_1 = ppg.FunctionInvariant("1", lambda: 55)
        job_2 = ppg.DataLoadingJob("2", lambda: None, depend_on_function=False)
        job_3 = ppg.DataLoadingJob("3", lambda: None, depend_on_function=False)
        job_4 = ppg.FileGeneratingJob("4", dummy_fg, depend_on_function=False)
        job_5 = ppg.SharedMultiFileGeneratingJob(
            "5", ["url.txt"], dummy_smfg, depend_on_function=False
        )
        job_6 = ppg.FunctionInvariant("6", lambda: 55)
        job_7 = ppg.ParameterInvariant("7", 55)
        job_8 = ppg.SharedMultiFileGeneratingJob(
            "8", ["genes.gtf"], dummy_smfg, depend_on_function=False
        )
        job_9 = ppg.FunctionInvariant("9", lambda: 55)
        job_10 = ppg.ParameterInvariant("10", 55)
        job_11 = ppg.SharedMultiFileGeneratingJob(
            "11",
            ["genome.fasta", "genome.fasta.fai"],
            dummy_smfg,
            depend_on_function=False,
        )
        job_12 = ppg.FunctionInvariant("12", lambda: 55)
        job_13 = ppg.ParameterInvariant("13", 55)
        job_14 = ppg.SharedMultiFileGeneratingJob(
            "14", ["references.txt"], dummy_smfg, depend_on_function=False
        )
        job_15 = ppg.FunctionInvariant("15", lambda: 55)
        job_16 = ppg.ParameterInvariant("16", 55)
        job_17 = ppg.SharedMultiFileGeneratingJob(
            "17",
            ["cdna.fasta", "cdna.fasta.fai"],
            dummy_smfg,
            depend_on_function=False,
        )
        job_18 = ppg.FunctionInvariant("18", lambda: 55)
        job_19 = ppg.ParameterInvariant("19", 55)
        job_20 = ppg.SharedMultiFileGeneratingJob(
            "20",
            ["pep.fasta", "pep.fasta.fai"],
            dummy_smfg,
            depend_on_function=False,
        )
        job_21 = ppg.FunctionInvariant("21", lambda: 55)
        job_22 = ppg.ParameterInvariant("22", 55)
        job_23 = ppg.SharedMultiFileGeneratingJob(
            "23", ["core.sql.gz"], dummy_smfg, depend_on_function=False
        )
        job_24 = ppg.FunctionInvariant("24", lambda: 55)
        job_25 = ppg.ParameterInvariant("25", 55)
        job_26 = ppg.SharedMultiFileGeneratingJob(
            "26", ["gene.txt.gz"], dummy_smfg, depend_on_function=False
        )
        job_27 = ppg.FunctionInvariant("27", lambda: 55)
        job_28 = ppg.ParameterInvariant("28", 55)
        job_29 = ppg.SharedMultiFileGeneratingJob(
            "29", ["transcript.txt.gz"], dummy_smfg, depend_on_function=False
        )
        job_30 = ppg.FunctionInvariant("30", lambda: 55)
        job_31 = ppg.ParameterInvariant("31", 55)
        job_32 = ppg.SharedMultiFileGeneratingJob(
            "32", ["translation.txt.gz"], dummy_smfg, depend_on_function=False
        )
        job_33 = ppg.FunctionInvariant("33", lambda: 55)
        job_34 = ppg.ParameterInvariant("34", 55)
        job_35 = ppg.SharedMultiFileGeneratingJob(
            "35", ["stable_id_event.txt.gz"], dummy_smfg, depend_on_function=False
        )
        job_36 = ppg.FunctionInvariant("36", lambda: 55)
        job_37 = ppg.ParameterInvariant("37", 55)
        job_38 = ppg.SharedMultiFileGeneratingJob(
            "38", ["external_db.txt.gz"], dummy_smfg, depend_on_function=False
        )
        job_39 = ppg.FunctionInvariant("39", lambda: 55)
        job_40 = ppg.ParameterInvariant("40", 55)
        job_41 = ppg.SharedMultiFileGeneratingJob(
            "41", ["object_xref.txt.gz"], dummy_smfg, depend_on_function=False
        )
        job_42 = ppg.FunctionInvariant("42", lambda: 55)
        job_43 = ppg.ParameterInvariant("43", 55)
        job_44 = ppg.SharedMultiFileGeneratingJob(
            "44", ["xref.txt.gz"], dummy_smfg, depend_on_function=False
        )
        job_45 = ppg.FunctionInvariant("45", lambda: 55)
        job_46 = ppg.ParameterInvariant("46", 55)
        job_47 = ppg.SharedMultiFileGeneratingJob(
            "47", ["alt_allele.txt.gz"], dummy_smfg, depend_on_function=False
        )
        job_48 = ppg.FunctionInvariant("48", lambda: 55)
        job_49 = ppg.ParameterInvariant("49", 55)
        job_50 = ppg.SharedMultiFileGeneratingJob(
            "50", ["seq_region.txt.gz"], dummy_smfg, depend_on_function=False
        )
        job_51 = ppg.FunctionInvariant("51", lambda: 55)
        job_52 = ppg.ParameterInvariant("52", 55)
        job_53 = ppg.SharedMultiFileGeneratingJob(
            "53", ["seq_region_attrib.txt.gz"], dummy_smfg, depend_on_function=False
        )
        job_54 = ppg.FunctionInvariant("54", lambda: 55)
        job_55 = ppg.ParameterInvariant("55", 55)
        job_56 = ppg.SharedMultiFileGeneratingJob(
            "56", ["attrib_type.txt.gz"], dummy_smfg, depend_on_function=False
        )
        job_57 = ppg.FunctionInvariant("57", lambda: 55)
        job_58 = ppg.ParameterInvariant("58", 55)
        job_59 = ppg.SharedMultiFileGeneratingJob(
            "59", ["external_synonym.txt.gz"], dummy_smfg, depend_on_function=False
        )
        job_60 = ppg.FunctionInvariant("60", lambda: 55)
        job_61 = ppg.ParameterInvariant("61", 55)
        job_62 = ppg.SharedMultiFileGeneratingJob(
            "62", ["df_genes.msgpack"], dummy_smfg, depend_on_function=False
        )
        job_63 = ppg.FunctionInvariant("63", lambda: 55)
        Path("64").write_text("A")
        job_64 = ppg.FileInvariant("64")
        job_65 = ppg.ParameterInvariant("65", 55)
        job_66 = ppg.FunctionInvariant("66", lambda: 55)
        job_67 = ppg.SharedMultiFileGeneratingJob(
            "67", ["df_transcripts.msgpack"], dummy_smfg, depend_on_function=False
        )
        job_68 = ppg.FunctionInvariant("68", lambda: 55)
        job_69 = ppg.ParameterInvariant("69", 55)
        job_70 = ppg.FunctionInvariant("70", lambda: 55)
        job_71 = ppg.FunctionInvariant("71", lambda: 55)
        job_72 = ppg.FunctionInvariant("72", lambda: 55)
        job_73 = ppg.ParameterInvariant("73", 55)
        job_74 = ppg.FunctionInvariant("74", lambda: 55)
        job_75 = ppg.FunctionInvariant("75", lambda: 55)
        job_76 = ppg.FileGeneratingJob("76", dummy_fg, depend_on_function=False)
        job_77 = ppg.FunctionInvariant("77", lambda: 55)
        job_78 = ppg.FunctionInvariant("78", lambda: 55)
        job_79 = ppg.AttributeLoadingJob(
            "79", DummyObject(), "attr_79", lambda: None, depend_on_function=False
        )
        job_80 = ppg.FileGeneratingJob("80", dummy_fg, depend_on_function=False)
        job_81 = ppg.FileGeneratingJob("81", dummy_fg, depend_on_function=False)
        job_82 = ppg.MultiFileGeneratingJob(
            ["82/Cont-1.bam", "82/sentinel.txt"],
            dummy_mfg,
            depend_on_function=False,
        )
        job_83 = ppg.TempFileGeneratingJob("83", dummy_fg, depend_on_function=False)
        Path("84").write_text("A")
        job_84 = ppg.FileInvariant("84")
        Path("85").write_text("A")
        job_85 = ppg.FileInvariant("85")
        Path("86").write_text("A")
        job_86 = ppg.FileInvariant("86")
        Path("87").write_text("A")
        job_87 = ppg.FileInvariant("87")
        job_88 = ppg.FunctionInvariant("88", lambda: 55)
        job_89 = ppg.ParameterInvariant("89", 55)
        job_90 = ppg.ParameterInvariant("90", 55)
        job_91 = ppg.SharedMultiFileGeneratingJob(
            "91",
            [
                "SA",
                "SAindex",
                "chrNameLength.txt",
                "exonGeTrInfo.tab",
                "exonInfo.tab",
                "geneInfo.tab",
                "sjdbInfo.txt",
                "sjdbList.fromGTF.out.tab",
                "sjdbList.out.tab",
                "transcriptInfo.tab",
            ],
            dummy_smfg,
            depend_on_function=False,
        )
        job_92 = ppg.FunctionInvariant("92", lambda: 55)
        job_93 = ppg.ParameterInvariant("93", 55)
        Path("94").write_text("A")
        job_94 = ppg.FileInvariant("94")
        job_95 = ppg.FunctionInvariant("95", lambda: 55)
        job_96 = ppg.FunctionInvariant("96", lambda: 55)
        job_97 = ppg.ParameterInvariant("97", 55)
        job_98 = ppg.FileGeneratingJob("98", dummy_fg, depend_on_function=False)
        job_99 = ppg.FunctionInvariant("99", lambda: 55)
        job_100 = ppg.FunctionInvariant("100", lambda: 55)
        job_101 = ppg.FunctionInvariant("101", lambda: 55)
        job_102 = ppg.ParameterInvariant("102", 55)
        job_103 = ppg.FileGeneratingJob("103", dummy_fg, depend_on_function=False)
        job_104 = ppg.FunctionInvariant("104", lambda: 55)
        job_105 = ppg.FunctionInvariant("105", lambda: 55)
        job_106 = ppg.FunctionInvariant("106", lambda: 55)
        job_107 = ppg.ParameterInvariant("107", 55)
        job_108 = ppg.FunctionInvariant("108", lambda: 55)
        job_109 = ppg.FunctionInvariant("109", lambda: 55)
        edges = []
        edges.append(("0", "1"))
        edges.append(("0", "2"))
        edges.append(("2", "3"))
        edges.append(("3", "4"))
        edges.append(("4", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("4", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("4", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("4", "14"))
        edges.append(("14", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("14", "15"))
        edges.append(("14", "16"))
        edges.append(("4", "17"))
        edges.append(("17", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("17", "18"))
        edges.append(("17", "19"))
        edges.append(("4", "20"))
        edges.append(("20", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("20", "21"))
        edges.append(("20", "22"))
        edges.append(("4", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("4", "26"))
        edges.append(("26", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("26", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("26", "27"))
        edges.append(("26", "28"))
        edges.append(("4", "29"))
        edges.append(("29", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("29", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("29", "30"))
        edges.append(("29", "31"))
        edges.append(("4", "32"))
        edges.append(("32", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("32", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("32", "33"))
        edges.append(("32", "34"))
        edges.append(("4", "35"))
        edges.append(("35", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("35", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("35", "36"))
        edges.append(("35", "37"))
        edges.append(("4", "38"))
        edges.append(("38", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("38", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("38", "39"))
        edges.append(("38", "40"))
        edges.append(("4", "41"))
        edges.append(("41", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("41", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("41", "42"))
        edges.append(("41", "43"))
        edges.append(("4", "44"))
        edges.append(("44", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("44", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("44", "45"))
        edges.append(("44", "46"))
        edges.append(("4", "47"))
        edges.append(("47", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("47", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("47", "48"))
        edges.append(("47", "49"))
        edges.append(("4", "50"))
        edges.append(("50", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("50", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("50", "51"))
        edges.append(("50", "52"))
        edges.append(("4", "53"))
        edges.append(("53", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("53", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("53", "54"))
        edges.append(("53", "55"))
        edges.append(("4", "56"))
        edges.append(("56", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("56", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("56", "57"))
        edges.append(("56", "58"))
        edges.append(("4", "59"))
        edges.append(("59", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("59", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("59", "60"))
        edges.append(("59", "61"))
        edges.append(("4", "62"))
        edges.append(("62", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("62", "63"))
        edges.append(("62", "64"))
        edges.append(("62", "65"))
        edges.append(("62", "66"))
        edges.append(("4", "67"))
        edges.append(("67", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("67", "62"))
        edges.append(("62", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("62", "63"))
        edges.append(("62", "64"))
        edges.append(("62", "65"))
        edges.append(("62", "66"))
        edges.append(("67", "64"))
        edges.append(("67", "68"))
        edges.append(("67", "69"))
        edges.append(("67", "70"))
        edges.append(("4", "71"))
        edges.append(("4", "72"))
        edges.append(("4", "73"))
        edges.append(("4", "74"))
        edges.append(("3", "75"))
        edges.append(("2", "76"))
        edges.append(("76", "3"))
        edges.append(("3", "4"))
        edges.append(("4", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("4", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("4", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("4", "14"))
        edges.append(("14", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("14", "15"))
        edges.append(("14", "16"))
        edges.append(("4", "17"))
        edges.append(("17", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("17", "18"))
        edges.append(("17", "19"))
        edges.append(("4", "20"))
        edges.append(("20", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("20", "21"))
        edges.append(("20", "22"))
        edges.append(("4", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("4", "26"))
        edges.append(("26", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("26", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("26", "27"))
        edges.append(("26", "28"))
        edges.append(("4", "29"))
        edges.append(("29", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("29", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("29", "30"))
        edges.append(("29", "31"))
        edges.append(("4", "32"))
        edges.append(("32", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("32", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("32", "33"))
        edges.append(("32", "34"))
        edges.append(("4", "35"))
        edges.append(("35", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("35", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("35", "36"))
        edges.append(("35", "37"))
        edges.append(("4", "38"))
        edges.append(("38", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("38", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("38", "39"))
        edges.append(("38", "40"))
        edges.append(("4", "41"))
        edges.append(("41", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("41", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("41", "42"))
        edges.append(("41", "43"))
        edges.append(("4", "44"))
        edges.append(("44", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("44", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("44", "45"))
        edges.append(("44", "46"))
        edges.append(("4", "47"))
        edges.append(("47", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("47", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("47", "48"))
        edges.append(("47", "49"))
        edges.append(("4", "50"))
        edges.append(("50", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("50", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("50", "51"))
        edges.append(("50", "52"))
        edges.append(("4", "53"))
        edges.append(("53", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("53", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("53", "54"))
        edges.append(("53", "55"))
        edges.append(("4", "56"))
        edges.append(("56", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("56", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("56", "57"))
        edges.append(("56", "58"))
        edges.append(("4", "59"))
        edges.append(("59", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("59", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("59", "60"))
        edges.append(("59", "61"))
        edges.append(("4", "62"))
        edges.append(("62", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("62", "63"))
        edges.append(("62", "64"))
        edges.append(("62", "65"))
        edges.append(("62", "66"))
        edges.append(("4", "67"))
        edges.append(("67", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("67", "62"))
        edges.append(("62", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("62", "63"))
        edges.append(("62", "64"))
        edges.append(("62", "65"))
        edges.append(("62", "66"))
        edges.append(("67", "64"))
        edges.append(("67", "68"))
        edges.append(("67", "69"))
        edges.append(("67", "70"))
        edges.append(("4", "71"))
        edges.append(("4", "72"))
        edges.append(("4", "73"))
        edges.append(("4", "74"))
        edges.append(("3", "75"))
        edges.append(("76", "77"))
        edges.append(("76", "78"))
        edges.append(("76", "79"))
        edges.append(("79", "80"))
        edges.append(("80", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("80", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("80", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("80", "14"))
        edges.append(("14", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("14", "15"))
        edges.append(("14", "16"))
        edges.append(("80", "17"))
        edges.append(("17", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("17", "18"))
        edges.append(("17", "19"))
        edges.append(("80", "20"))
        edges.append(("20", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("20", "21"))
        edges.append(("20", "22"))
        edges.append(("80", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("80", "26"))
        edges.append(("26", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("26", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("26", "27"))
        edges.append(("26", "28"))
        edges.append(("80", "29"))
        edges.append(("29", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("29", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("29", "30"))
        edges.append(("29", "31"))
        edges.append(("80", "32"))
        edges.append(("32", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("32", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("32", "33"))
        edges.append(("32", "34"))
        edges.append(("80", "35"))
        edges.append(("35", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("35", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("35", "36"))
        edges.append(("35", "37"))
        edges.append(("80", "38"))
        edges.append(("38", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("38", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("38", "39"))
        edges.append(("38", "40"))
        edges.append(("80", "41"))
        edges.append(("41", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("41", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("41", "42"))
        edges.append(("41", "43"))
        edges.append(("80", "44"))
        edges.append(("44", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("44", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("44", "45"))
        edges.append(("44", "46"))
        edges.append(("80", "47"))
        edges.append(("47", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("47", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("47", "48"))
        edges.append(("47", "49"))
        edges.append(("80", "50"))
        edges.append(("50", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("50", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("50", "51"))
        edges.append(("50", "52"))
        edges.append(("80", "53"))
        edges.append(("53", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("53", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("53", "54"))
        edges.append(("53", "55"))
        edges.append(("80", "56"))
        edges.append(("56", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("56", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("56", "57"))
        edges.append(("56", "58"))
        edges.append(("80", "59"))
        edges.append(("59", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("59", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("59", "60"))
        edges.append(("59", "61"))
        edges.append(("80", "81"))
        edges.append(("81", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("81", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("81", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("81", "14"))
        edges.append(("14", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("14", "15"))
        edges.append(("14", "16"))
        edges.append(("81", "17"))
        edges.append(("17", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("17", "18"))
        edges.append(("17", "19"))
        edges.append(("81", "20"))
        edges.append(("20", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("20", "21"))
        edges.append(("20", "22"))
        edges.append(("81", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("81", "26"))
        edges.append(("26", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("26", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("26", "27"))
        edges.append(("26", "28"))
        edges.append(("81", "29"))
        edges.append(("29", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("29", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("29", "30"))
        edges.append(("29", "31"))
        edges.append(("81", "32"))
        edges.append(("32", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("32", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("32", "33"))
        edges.append(("32", "34"))
        edges.append(("81", "35"))
        edges.append(("35", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("35", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("35", "36"))
        edges.append(("35", "37"))
        edges.append(("81", "38"))
        edges.append(("38", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("38", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("38", "39"))
        edges.append(("38", "40"))
        edges.append(("81", "41"))
        edges.append(("41", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("41", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("41", "42"))
        edges.append(("41", "43"))
        edges.append(("81", "44"))
        edges.append(("44", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("44", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("44", "45"))
        edges.append(("44", "46"))
        edges.append(("81", "47"))
        edges.append(("47", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("47", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("47", "48"))
        edges.append(("47", "49"))
        edges.append(("81", "50"))
        edges.append(("50", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("50", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("50", "51"))
        edges.append(("50", "52"))
        edges.append(("81", "53"))
        edges.append(("53", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("53", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("53", "54"))
        edges.append(("53", "55"))
        edges.append(("81", "56"))
        edges.append(("56", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("56", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("56", "57"))
        edges.append(("56", "58"))
        edges.append(("81", "59"))
        edges.append(("59", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("59", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("59", "60"))
        edges.append(("59", "61"))
        edges.append(("81", "82"))
        edges.append(("82", "83"))
        edges.append(("83", "84"))
        edges.append(("83", "85"))
        edges.append(("83", "86"))
        edges.append(("83", "87"))
        edges.append(("83", "88"))
        edges.append(("83", "89"))
        edges.append(("83", "90"))
        edges.append(("82", "91"))
        edges.append(("91", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("91", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("91", "92"))
        edges.append(("91", "93"))
        edges.append(("82", "94"))
        edges.append(("82", "95"))
        edges.append(("82", "96"))
        edges.append(("82", "97"))
        edges.append(("81", "98"))
        edges.append(("98", "82"))
        edges.append(("82", "83"))
        edges.append(("83", "84"))
        edges.append(("83", "85"))
        edges.append(("83", "86"))
        edges.append(("83", "87"))
        edges.append(("83", "88"))
        edges.append(("83", "89"))
        edges.append(("83", "90"))
        edges.append(("82", "91"))
        edges.append(("91", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("91", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("91", "92"))
        edges.append(("91", "93"))
        edges.append(("82", "94"))
        edges.append(("82", "95"))
        edges.append(("82", "96"))
        edges.append(("82", "97"))
        edges.append(("98", "99"))
        edges.append(("81", "100"))
        edges.append(("81", "101"))
        edges.append(("81", "102"))
        edges.append(("80", "103"))
        edges.append(("103", "81"))
        edges.append(("81", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("81", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("81", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("81", "14"))
        edges.append(("14", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("14", "15"))
        edges.append(("14", "16"))
        edges.append(("81", "17"))
        edges.append(("17", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("17", "18"))
        edges.append(("17", "19"))
        edges.append(("81", "20"))
        edges.append(("20", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("20", "21"))
        edges.append(("20", "22"))
        edges.append(("81", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("81", "26"))
        edges.append(("26", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("26", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("26", "27"))
        edges.append(("26", "28"))
        edges.append(("81", "29"))
        edges.append(("29", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("29", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("29", "30"))
        edges.append(("29", "31"))
        edges.append(("81", "32"))
        edges.append(("32", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("32", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("32", "33"))
        edges.append(("32", "34"))
        edges.append(("81", "35"))
        edges.append(("35", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("35", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("35", "36"))
        edges.append(("35", "37"))
        edges.append(("81", "38"))
        edges.append(("38", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("38", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("38", "39"))
        edges.append(("38", "40"))
        edges.append(("81", "41"))
        edges.append(("41", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("41", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("41", "42"))
        edges.append(("41", "43"))
        edges.append(("81", "44"))
        edges.append(("44", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("44", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("44", "45"))
        edges.append(("44", "46"))
        edges.append(("81", "47"))
        edges.append(("47", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("47", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("47", "48"))
        edges.append(("47", "49"))
        edges.append(("81", "50"))
        edges.append(("50", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("50", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("50", "51"))
        edges.append(("50", "52"))
        edges.append(("81", "53"))
        edges.append(("53", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("53", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("53", "54"))
        edges.append(("53", "55"))
        edges.append(("81", "56"))
        edges.append(("56", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("56", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("56", "57"))
        edges.append(("56", "58"))
        edges.append(("81", "59"))
        edges.append(("59", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("59", "23"))
        edges.append(("23", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("23", "24"))
        edges.append(("23", "25"))
        edges.append(("59", "60"))
        edges.append(("59", "61"))
        edges.append(("81", "82"))
        edges.append(("82", "83"))
        edges.append(("83", "84"))
        edges.append(("83", "85"))
        edges.append(("83", "86"))
        edges.append(("83", "87"))
        edges.append(("83", "88"))
        edges.append(("83", "89"))
        edges.append(("83", "90"))
        edges.append(("82", "91"))
        edges.append(("91", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("91", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("91", "92"))
        edges.append(("91", "93"))
        edges.append(("82", "94"))
        edges.append(("82", "95"))
        edges.append(("82", "96"))
        edges.append(("82", "97"))
        edges.append(("81", "98"))
        edges.append(("98", "82"))
        edges.append(("82", "83"))
        edges.append(("83", "84"))
        edges.append(("83", "85"))
        edges.append(("83", "86"))
        edges.append(("83", "87"))
        edges.append(("83", "88"))
        edges.append(("83", "89"))
        edges.append(("83", "90"))
        edges.append(("82", "91"))
        edges.append(("91", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("91", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("91", "92"))
        edges.append(("91", "93"))
        edges.append(("82", "94"))
        edges.append(("82", "95"))
        edges.append(("82", "96"))
        edges.append(("82", "97"))
        edges.append(("98", "99"))
        edges.append(("81", "100"))
        edges.append(("81", "101"))
        edges.append(("81", "102"))
        edges.append(("103", "104"))
        edges.append(("80", "105"))
        edges.append(("79", "106"))
        edges.append(("76", "107"))
        edges.append(("76", "108"))
        edges.append(("2", "109"))
        for a, b in edges:
            if a in ppg.global_pipegraph.jobs and b in ppg.global_pipegraph.jobs:
                ppg.global_pipegraph.jobs[a].depends_on(ppg.global_pipegraph.jobs[b])

        ppg.run()
        ppg.run()
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("91", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("91", "92"))
        edges.append(("91", "93"))
        edges.append(("82", "94"))
        edges.append(("82", "95"))
        edges.append(("82", "96"))
        edges.append(("82", "97"))
        edges.append(("81", "98"))
        edges.append(("98", "82"))
        edges.append(("82", "83"))
        edges.append(("83", "84"))
        edges.append(("83", "85"))
        edges.append(("83", "86"))
        edges.append(("83", "87"))
        edges.append(("83", "88"))
        edges.append(("83", "89"))
        edges.append(("83", "90"))
        edges.append(("82", "91"))
        edges.append(("91", "8"))
        edges.append(("8", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("8", "9"))
        edges.append(("8", "10"))
        edges.append(("91", "11"))
        edges.append(("11", "5"))
        edges.append(("5", "6"))
        edges.append(("5", "7"))
        edges.append(("11", "12"))
        edges.append(("11", "13"))
        edges.append(("91", "92"))
        edges.append(("91", "93"))
        edges.append(("82", "94"))
        edges.append(("82", "95"))
        edges.append(("82", "96"))
        edges.append(("82", "97"))
        edges.append(("98", "99"))
        edges.append(("81", "100"))
        edges.append(("81", "101"))
        edges.append(("81", "102"))
        edges.append(("103", "104"))
        edges.append(("80", "105"))
        edges.append(("79", "106"))
        edges.append(("76", "107"))
        edges.append(("76", "108"))
        edges.append(("2", "109"))
        for a, b in edges:
            if a in ppg.global_pipegraph.jobs and b in ppg.global_pipegraph.jobs:
                ppg.global_pipegraph.jobs[a].depends_on(ppg.global_pipegraph.jobs[b])

        ppg.run()
        ppg.run()

    def test_20210729(self, job_trace_log):
        def build():
            jobs_by_no = {}

            job_0 = ppg.FileGeneratingJob("0", dummy_fg, depend_on_function=False)
            jobs_by_no["0"] = job_0

            job_1 = ppg.DataLoadingJob("1", lambda: 55, depend_on_function=False)
            jobs_by_no["1"] = job_1

            job_530 = ppg.DataLoadingJob("530", lambda: 55, depend_on_function=False)
            jobs_by_no["530"] = job_530

            job_541 = ppg.DataLoadingJob("541", lambda: 55, depend_on_function=False)
            jobs_by_no["541"] = job_541

            job_542 = ppg.FileGeneratingJob("542", dummy_fg, depend_on_function=False)
            jobs_by_no["542"] = job_542

            edges = [
                ("0", "541"),
                ("0", "530"),
                ("542", "530"),
                ("542", "1"),
                ("541", "1"),
                ("541", "542"),
            ]

            ok_edges = []
            for a, b in edges:
                if a in jobs_by_no and b in jobs_by_no:
                    jobs_by_no[a].depends_on(jobs_by_no[b])
                    ok_edges.append((a, b))

        ppg.new(allow_short_filenames=True)
        build()
        ppg.run()
        ppg.new(allow_short_filenames=True, log_level=6)
        build()
        ppg.run()

    def test_ttcc(self):
        """The absolute minimal two terminal jobs, two conditional ones,
        each T depending on each C, cross over
        """
        t1 = ppg.FileGeneratingJob(
            "t1", lambda of: of.write_text(of.name) and counter("ct1")
        )
        t2 = ppg.FileGeneratingJob(
            "t2", lambda of: of.write_text(of.name) and counter("ct2")
        )
        # these do not invalidate...
        c1 = ppg.DataLoadingJob("c1", lambda: 1, depend_on_function=False)
        c2 = ppg.DataLoadingJob("c2", lambda: 1, depend_on_function=False)

        t1.depends_on(c1, c2)
        t2.depends_on(c1, c2)
        ppg.run()
        assert read("t1") == "t1"
        assert read("ct1") == "1"
        ppg.run()
        assert read("ct1") == "1"
        assert read("ct1") == "1"

        Path("t1").unlink()
        ppg.run()

        assert read("t1") == "t1"
        assert read("ct1") == "2"
        assert read("ct2") == "1"

        c1 = ppg.DataLoadingJob("c1", lambda: 2, depend_on_function=False)
        ppg.run()
        # w ehad no function invariant1
        assert read("ct1") == "2"
        assert read("ct2") == "1"
        Path(
            "t2"
        ).unlink()  # but now it runs, and it invalidates, and they *both* need to rerun!
        ppg.run()
        assert read("ct1") == "3"
        assert read("ct2") == "2"

        ppg.run()  # no change
        assert read("ct1") == "3"
        assert read("ct2") == "2"

        Path("t2").unlink()
        ppg.run()
        assert read("ct2") == "3"  # only t2 get's run...

    def test_20211001(self, job_trace_log):
        do_fail = False

        def fail():  # fail on demand
            if do_fail:
                raise ValueError()

        job_3 = ppg.DataLoadingJob("3", lambda: None, depend_on_function=False)
        job_48 = ppg.AttributeLoadingJob(
            "48", DummyObject(), "attr_48", fail, depend_on_function=False
        )
        job_61 = ppg.FileGeneratingJob("61", dummy_fg, depend_on_function=False)
        job_67 = ppg.JobGeneratingJob("67", lambda: None, depend_on_function=False)

        edges = []

        edges.append(("61", "48"))
        edges.append(("67", "48"))
        edges.append(("61", "3"))

        for a, b in edges:
            if a in ppg.global_pipegraph.jobs and b in ppg.global_pipegraph.jobs:
                ppg.global_pipegraph.jobs[a].depends_on(ppg.global_pipegraph.jobs[b])

        ppg.run()
        ppg.run()
        do_fail = True
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert (
            ppg.global_pipegraph.last_run_result["48"].outcome
            == ppg.enums.JobOutcome.Failed
        )
        assert (
            ppg.global_pipegraph.last_run_result["61"].outcome
            == ppg.enums.JobOutcome.UpstreamFailed
        )
        assert (
            ppg.global_pipegraph.last_run_result["61"].outcome
            == ppg.enums.JobOutcome.UpstreamFailed
        )
        assert (
            ppg.global_pipegraph.last_run_result["3"].outcome
            == ppg.enums.JobOutcome.Skipped
        )

    def test_20211221(self):
        global do_fail
        do_fail = [False]
        # ppg.new(log_level=20)
        gen_20211221(lambda: 55)
        ppg.run()
        assert Path("651").exists()
        ppg.new(log_level=5)
        do_fail[0] = True
        gen_20211221(lambda: 56)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert not Path("651").exists()
        assert (
            ppg.global_pipegraph.last_run_result["651"].outcome
            == ppg.enums.JobOutcome.Failed
        )
        assert (
            ppg.global_pipegraph.last_run_result["1079"].outcome
            == ppg.enums.JobOutcome.UpstreamFailed
        )
        assert (
            ppg.global_pipegraph.last_run_result["1096"].outcome
            == ppg.enums.JobOutcome.UpstreamFailed
        )
        assert (
            ppg.global_pipegraph.last_run_result["661"].outcome
            == ppg.enums.JobOutcome.Skipped
        )

    def test_cleanup_already_decided_to_skip_upstream_failed(self):
        # debugged job CleanUp:cache/lanes/test/input.fastq
        # this makes the TF run.
        job_1 = ppg.FileGeneratingJob("1", dummy_fg, depend_on_function=False)
        # this one introduceds a cleanup.
        job_2 = ppg.TempFileGeneratingJob("2", dummy_fg, depend_on_function=False)

        job_8 = ppg.FileGeneratingJob("8", dummy_fg, depend_on_function=False)

        # this one we have fail in the second round
        job_11 = ppg.FileGeneratingJob("11", dummy_fg, depend_on_function=True)

        cjobs_by_no = {}
        for k, v in locals().items():
            if k.startswith("job_"):
                no = k[k.find("_") + 1 :]
                cjobs_by_no[no] = v
        edges = []
        ea = edges.append
        ea(("1", "2"))
        ea(("8", "11"))
        ea(("8", "2"))
        for a, b in edges:
            if a in cjobs_by_no and b in cjobs_by_no:
                cjobs_by_no[a].depends_on(cjobs_by_no[b])
                # print(f"ea(('{a}', '{b}'))")

        ppg.run()

        # now make it fail
        ppg.new()  # log_level=6)
        # as above
        job_1 = ppg.FileGeneratingJob("1", dummy_fg, depend_on_function=False)
        job_2 = ppg.TempFileGeneratingJob("2", dummy_fg, depend_on_function=False)
        job_8 = ppg.FileGeneratingJob("8", dummy_fg, depend_on_function=False)

        # make this one fail.
        job_11 = ppg.FileGeneratingJob("11", dummy_fg_raising, depend_on_function=True)
        for a, b in edges:
            if a in cjobs_by_no and b in cjobs_by_no:
                cjobs_by_no[a].depends_on(cjobs_by_no[b])

        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert (
            ppg.global_pipegraph.last_run_result["11"].outcome
            == ppg.enums.JobOutcome.Failed
        )
        assert (
            ppg.global_pipegraph.last_run_result["2"].outcome
            == ppg.enums.JobOutcome.Skipped
        )
        assert (
            ppg.global_pipegraph.last_run_result["CleanUp:2"].outcome
            == ppg.enums.JobOutcome.UpstreamFailed  # 11 fails, which fails 8, which fails this cleanup
        )

        # and boom, job was marked done & skipped, but we now inform it it's upstream failed.

    def test_depends_on_mfg_by_str_job_id_does_not_work(self):
        """Verify that you can't depend_on(job_id) off a multi file generating job,
        but you can depend on the individual files.

        """
        a = ppg.FileGeneratingJob("a", lambda of: of.write_text("a"))

        def do_b(ofs):
            for fn in ofs:
                fn.write_text(fn.name)

        b = ppg.MultiFileGeneratingJob(["b", "c"], do_b)
        # aassert ppg.global_pipegraph.find_job_from_file(b.job_id) is b
        with pytest.raises(KeyError):
            a.depends_on(b.job_id)
        assert ppg.global_pipegraph.find_job_from_file("b") is b
        assert ppg.global_pipegraph.find_job_from_file("c") is b

    def test_returning_dataframe_from_dataloading(self):
        import pandas as pd

        a = (pd.DataFrame({"a": [1]}), pd.DataFrame({"b": [1, 3, 3]}))
        b = (pd.DataFrame({"a": [1, 2]}), pd.DataFrame({"b": [1, 3, 3]}))
        hash_a = ppg.jobs._hash_object(a)[1]
        hash_b = ppg.jobs._hash_object(b)[1]
        print(hash_a)
        print(hash_b)
        assert hash_a != hash_b

        ppg.new(log_level=5)
        storage = {}

        def dl():
            res = pd.DataFrame({"a": [1]})
            storage["dl"] = res
            storage["dl2"] = pd.DataFrame({"b": [1, 3, 3]})
            return storage["dl"], storage["dl2"]

        def doit(filename, storage=storage):
            counter("doit")
            filename.write_text(str(len(storage["dl"]) + len(storage["dl2"])))

        a = ppg.DataLoadingJob("a", dl)
        b = ppg.FileGeneratingJob("b", doit)
        b.depends_on(a)
        ppg.run()
        assert Path("doit").read_text() == "1"
        assert Path("b").read_text() == "4"
        ppg.new(log_level=5)

        def dl():  # same output
            res = pd.DataFrame({"a": [1]})
            storage["dl"] = res
            storage["dl2"] = pd.DataFrame({"b": [1, 3, 3]})
            return storage["dl"], storage["dl2"]

        a = ppg.DataLoadingJob("a", dl)
        b = ppg.FileGeneratingJob("b", doit)
        b.depends_on(a)
        ppg.run()
        assert Path("doit").read_text() == "1"
        assert Path("b").read_text() == "4"

        ppg.new()

        def dl():
            res = pd.DataFrame({"a": [1, 2]})
            storage["dl"] = res
            storage["dl2"] = pd.DataFrame({"b": [1, 3, 3]})
            return storage["dl"], storage["dl2"]

        a = ppg.DataLoadingJob("a", dl)
        b = ppg.FileGeneratingJob("b", doit)
        b.depends_on(a)
        ppg.run()
        assert Path("doit").read_text() == "2"
        assert Path("b").read_text() == "5"


def gen_20211221(func):
    global do_fail

    def dummy_fg_fail(of):
        global do_fail
        if do_fail[0]:
            raise ValueError()
        of.parent.mkdir(exist_ok=True, parents=True)
        of.write_text("fg")

    # debugged job PIGenes_KD5MA closest genes_parent
    # debugged job load_cache/GenomicRegions/H4ac_ISX_specific/calc

    job_650 = ppg.DataLoadingJob("650", lambda: 35, depend_on_function=False)
    # debugged job cache/GenomicRegions/H4ac_ISX_specific/calc
    job_651 = ppg.FileGeneratingJob("651", dummy_fg_fail, depend_on_function=False)
    job_651.depends_on(ppg.FunctionInvariant("shu", func))
    job_661 = ppg.DataLoadingJob("661", lambda: 35, depend_on_function=False)
    job_1079 = ppg.FileGeneratingJob("1079", dummy_fg, depend_on_function=False)
    job_1096 = ppg.DataLoadingJob("1096", lambda: 35, depend_on_function=False)

    cjobs_by_no = {}
    for k, v in locals().items():
        if k.startswith("job_"):
            no = k[k.find("_") + 1 :]
            cjobs_by_no[no] = v
    edges = []
    ea = edges.append
    ea(("1079", "1096"))
    ea(("1096", "650"))
    ea(("1096", "661"))
    ea(("650", "651"))
    for a, b in edges:
        if a in cjobs_by_no and b in cjobs_by_no:
            cjobs_by_no[a].depends_on(cjobs_by_no[b])
            # print(f"ea(('{a}', '{b}'))")


def test_pandas_hashing():
    import pandas as pd

    # we only care about the hash.
    hf = lambda x: ppg.jobs._hash_object(x)[1]

    df_a = pd.DataFrame(
        {"A": [1, 2, 3], "B": ["4", "5", "6"]},
        index=pd.MultiIndex.from_tuples(
            [(1, "2"), (3, "4"), (5, "6")], names=["a", "b"]
        ),
    )
    df_a2 = pd.DataFrame(
        {"A": [1, 2, 3], "B": ["4", "5", "6"]},
        index=pd.MultiIndex.from_tuples(
            [(1, "2"), (3, "4"), (5, "6")], names=["a", "b"]
        ),
    )
    df_b = pd.DataFrame(
        {"A": [1, 2, 4], "B": ["4", "5", "6"]},
        index=pd.MultiIndex.from_tuples(
            [(1, "2"), (3, "4"), (5, "6")], names=["a", "b"]
        ),
    )
    df_c = pd.DataFrame(
        {"A": [1, 2, 3], "B": ["x", "5", "6"]},
        index=pd.MultiIndex.from_tuples(
            [(1, "2"), (3, "4"), (5, "6")], names=["a", "b"]
        ),
    )
    df_d = pd.DataFrame(
        {"A": [1, 2, 3], "B": ["4", "5", "6"]},
        index=pd.MultiIndex.from_tuples(
            [(2, "2"), (3, "4"), (5, "6")], names=["a", "b"]
        ),
    )
    df_e = pd.DataFrame(
        {"A": [1, 2, 3], "B": ["4", "5", "6"]},
        index=pd.MultiIndex.from_tuples([(1, 2), (3, 4), (5, 6)], names=["a", "b"]),
    )
    df_f = pd.DataFrame(
        {"A": [1.0, 2.0, 3.0], "B": ["4", "5", "6"]},
        index=pd.MultiIndex.from_tuples(
            [(1, "2"), (3, "4"), (5, "6")], names=["a", "b"]
        ),
    )
    df_g = pd.DataFrame({"A": [1, 2, 3], "B": ["4", "5", "6"]}, index=[1, 3, 5])
    # we are not checking the names of indices
    # df_h = pd.DataFrame(
    #     {"A": [1, 2, 3], "B": ["4", "5", "6"]},
    #     index=pd.MultiIndex.from_tuples([(1, "2"), (3, "4"), (5, "6")]),
    # )

    assert hf(df_a) == hf(df_a)
    assert hf(df_a) == hf(df_a2)
    assert hf(df_a) != hf(df_b)
    assert hf(df_a) != hf(df_c)
    assert hf(df_a) != hf(df_d)
    assert hf(df_a) != hf(df_e)
    assert hf(df_a) != hf(df_f)
    assert hf(df_a) != hf(df_g)
    # assert hf(df_a) != hf(df_h)
