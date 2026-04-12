// Auto-generated from s3://hl-mainnet-node-data/replica_cmds (requester pays).
// Coverage: 20250720..20260331.

pub(crate) const REPLICA_INDEX_START_DATE_YYYYMMDD: &str = "20250720";
pub(crate) const REPLICA_INDEX_END_DATE_YYYYMMDD: &str = "20260331";
pub(crate) const REPLICA_INDEX_END_EXCLUSIVE_CHUNK: u64 = 941_780_000; // 20260401 start

pub(crate) const REPLICA_SNAPSHOT_DIRS: [&str; 32] = [
    "2025-07-19T10:13:42Z",
    "2025-07-27T12:00:27Z",
    "2025-08-02T09:18:00Z",
    "2025-08-09T08:16:44Z",
    "2025-08-16T08:22:05Z",
    "2025-08-23T08:56:46Z",
    "2025-08-30T08:20:04Z",
    "2025-09-06T08:31:30Z",
    "2025-09-13T09:17:43Z",
    "2025-09-21T06:34:21Z",
    "2025-09-27T09:28:27Z",
    "2025-10-12T08:02:27Z",
    "2025-10-13T10:03:09Z",
    "2025-10-18T08:30:59Z",
    "2025-10-25T08:03:24Z",
    "2025-11-01T07:32:59Z",
    "2025-11-08T08:12:47Z",
    "2025-11-15T08:06:02Z",
    "2025-11-22T09:21:51Z",
    "2025-11-29T08:48:22Z",
    "2025-12-06T08:07:53Z",
    "2025-12-20T07:47:44Z",
    "2026-01-03T07:05:33Z",
    "2026-01-17T07:05:45Z",
    "2026-01-24T08:02:26Z",
    "2026-01-31T08:04:34Z",
    "2026-02-07T07:04:22Z",
    "2026-02-16T12:21:56Z",
    "2026-02-21T07:34:29Z",
    "2026-03-01T07:06:51Z",
    "2026-03-14T07:05:59Z",
    "2026-03-21T09:33:29Z",
];

#[derive(Clone, Copy, Debug)]
pub(crate) struct ReplicaDayIndexEntry {
    pub(crate) snapshot_idx: u8,
    pub(crate) start_chunk: u64,
}

pub(crate) const REPLICA_DAY_INDEX: [ReplicaDayIndexEntry; 255] = [
    ReplicaDayIndexEntry { snapshot_idx: 0, start_chunk: 668_430_000 }, // 20250720 2025-07-19T10:13:42Z
    ReplicaDayIndexEntry { snapshot_idx: 0, start_chunk: 669_550_000 }, // 20250721 2025-07-19T10:13:42Z
    ReplicaDayIndexEntry { snapshot_idx: 0, start_chunk: 670_670_000 }, // 20250722 2025-07-19T10:13:42Z
    ReplicaDayIndexEntry { snapshot_idx: 0, start_chunk: 671_770_000 }, // 20250723 2025-07-19T10:13:42Z
    ReplicaDayIndexEntry { snapshot_idx: 0, start_chunk: 672_880_000 }, // 20250724 2025-07-19T10:13:42Z
    ReplicaDayIndexEntry { snapshot_idx: 0, start_chunk: 673_970_000 }, // 20250725 2025-07-19T10:13:42Z
    ReplicaDayIndexEntry { snapshot_idx: 0, start_chunk: 675_070_000 }, // 20250726 2025-07-19T10:13:42Z
    ReplicaDayIndexEntry { snapshot_idx: 1, start_chunk: 676_714_000 }, // 20250727 2025-07-27T12:00:27Z
    ReplicaDayIndexEntry { snapshot_idx: 1, start_chunk: 677_280_000 }, // 20250728 2025-07-27T12:00:27Z
    ReplicaDayIndexEntry { snapshot_idx: 1, start_chunk: 678_380_000 }, // 20250729 2025-07-27T12:00:27Z
    ReplicaDayIndexEntry { snapshot_idx: 1, start_chunk: 679_470_000 }, // 20250730 2025-07-27T12:00:27Z
    ReplicaDayIndexEntry { snapshot_idx: 1, start_chunk: 680_560_000 }, // 20250731 2025-07-27T12:00:27Z
    ReplicaDayIndexEntry { snapshot_idx: 1, start_chunk: 681_650_000 }, // 20250801 2025-07-27T12:00:27Z
    ReplicaDayIndexEntry { snapshot_idx: 2, start_chunk: 683_143_000 }, // 20250802 2025-08-02T09:18:00Z
    ReplicaDayIndexEntry { snapshot_idx: 2, start_chunk: 683_810_000 }, // 20250803 2025-08-02T09:18:00Z
    ReplicaDayIndexEntry { snapshot_idx: 2, start_chunk: 684_920_000 }, // 20250804 2025-08-02T09:18:00Z
    ReplicaDayIndexEntry { snapshot_idx: 2, start_chunk: 686_030_000 }, // 20250805 2025-08-02T09:18:00Z
    ReplicaDayIndexEntry { snapshot_idx: 2, start_chunk: 687_140_000 }, // 20250806 2025-08-02T09:18:00Z
    ReplicaDayIndexEntry { snapshot_idx: 2, start_chunk: 688_230_000 }, // 20250807 2025-08-02T09:18:00Z
    ReplicaDayIndexEntry { snapshot_idx: 2, start_chunk: 689_340_000 }, // 20250808 2025-08-02T09:18:00Z
    ReplicaDayIndexEntry { snapshot_idx: 3, start_chunk: 690_826_000 }, // 20250809 2025-08-09T08:16:44Z
    ReplicaDayIndexEntry { snapshot_idx: 3, start_chunk: 691_550_000 }, // 20250810 2025-08-09T08:16:44Z
    ReplicaDayIndexEntry { snapshot_idx: 3, start_chunk: 692_650_000 }, // 20250811 2025-08-09T08:16:44Z
    ReplicaDayIndexEntry { snapshot_idx: 3, start_chunk: 693_740_000 }, // 20250812 2025-08-09T08:16:44Z
    ReplicaDayIndexEntry { snapshot_idx: 3, start_chunk: 694_830_000 }, // 20250813 2025-08-09T08:16:44Z
    ReplicaDayIndexEntry { snapshot_idx: 3, start_chunk: 695_910_000 }, // 20250814 2025-08-09T08:16:44Z
    ReplicaDayIndexEntry { snapshot_idx: 3, start_chunk: 696_970_000 }, // 20250815 2025-08-09T08:16:44Z
    ReplicaDayIndexEntry { snapshot_idx: 4, start_chunk: 698_441_000 }, // 20250816 2025-08-16T08:22:05Z
    ReplicaDayIndexEntry { snapshot_idx: 4, start_chunk: 699_180_000 }, // 20250817 2025-08-16T08:22:05Z
    ReplicaDayIndexEntry { snapshot_idx: 4, start_chunk: 700_300_000 }, // 20250818 2025-08-16T08:22:05Z
    ReplicaDayIndexEntry { snapshot_idx: 4, start_chunk: 701_390_000 }, // 20250819 2025-08-16T08:22:05Z
    ReplicaDayIndexEntry { snapshot_idx: 4, start_chunk: 702_470_000 }, // 20250820 2025-08-16T08:22:05Z
    ReplicaDayIndexEntry { snapshot_idx: 4, start_chunk: 703_550_000 }, // 20250821 2025-08-16T08:22:05Z
    ReplicaDayIndexEntry { snapshot_idx: 4, start_chunk: 704_640_000 }, // 20250822 2025-08-16T08:22:05Z
    ReplicaDayIndexEntry { snapshot_idx: 5, start_chunk: 706_113_000 }, // 20250823 2025-08-23T08:56:46Z
    ReplicaDayIndexEntry { snapshot_idx: 5, start_chunk: 706_810_000 }, // 20250824 2025-08-23T08:56:46Z
    ReplicaDayIndexEntry { snapshot_idx: 5, start_chunk: 707_920_000 }, // 20250825 2025-08-23T08:56:46Z
    ReplicaDayIndexEntry { snapshot_idx: 5, start_chunk: 709_010_000 }, // 20250826 2025-08-23T08:56:46Z
    ReplicaDayIndexEntry { snapshot_idx: 5, start_chunk: 710_110_000 }, // 20250827 2025-08-23T08:56:46Z
    ReplicaDayIndexEntry { snapshot_idx: 5, start_chunk: 711_210_000 }, // 20250828 2025-08-23T08:56:46Z
    ReplicaDayIndexEntry { snapshot_idx: 5, start_chunk: 712_310_000 }, // 20250829 2025-08-23T08:56:46Z
    ReplicaDayIndexEntry { snapshot_idx: 6, start_chunk: 713_771_000 }, // 20250830 2025-08-30T08:20:04Z
    ReplicaDayIndexEntry { snapshot_idx: 6, start_chunk: 714_490_000 }, // 20250831 2025-08-30T08:20:04Z
    ReplicaDayIndexEntry { snapshot_idx: 6, start_chunk: 715_590_000 }, // 20250901 2025-08-30T08:20:04Z
    ReplicaDayIndexEntry { snapshot_idx: 6, start_chunk: 716_660_000 }, // 20250902 2025-08-30T08:20:04Z
    ReplicaDayIndexEntry { snapshot_idx: 6, start_chunk: 717_740_000 }, // 20250903 2025-08-30T08:20:04Z
    ReplicaDayIndexEntry { snapshot_idx: 6, start_chunk: 718_820_000 }, // 20250904 2025-08-30T08:20:04Z
    ReplicaDayIndexEntry { snapshot_idx: 6, start_chunk: 719_900_000 }, // 20250905 2025-08-30T08:20:04Z
    ReplicaDayIndexEntry { snapshot_idx: 7, start_chunk: 721_349_000 }, // 20250906 2025-09-06T08:31:30Z
    ReplicaDayIndexEntry { snapshot_idx: 7, start_chunk: 722_050_000 }, // 20250907 2025-09-06T08:31:30Z
    ReplicaDayIndexEntry { snapshot_idx: 7, start_chunk: 723_140_000 }, // 20250908 2025-09-06T08:31:30Z
    ReplicaDayIndexEntry { snapshot_idx: 7, start_chunk: 724_230_000 }, // 20250909 2025-09-06T08:31:30Z
    ReplicaDayIndexEntry { snapshot_idx: 7, start_chunk: 725_300_000 }, // 20250910 2025-09-06T08:31:30Z
    ReplicaDayIndexEntry { snapshot_idx: 7, start_chunk: 726_390_000 }, // 20250911 2025-09-06T08:31:30Z
    ReplicaDayIndexEntry { snapshot_idx: 7, start_chunk: 727_480_000 }, // 20250912 2025-09-06T08:31:30Z
    ReplicaDayIndexEntry { snapshot_idx: 8, start_chunk: 728_982_000 }, // 20250913 2025-09-13T09:17:43Z
    ReplicaDayIndexEntry { snapshot_idx: 8, start_chunk: 729_660_000 }, // 20250914 2025-09-13T09:17:43Z
    ReplicaDayIndexEntry { snapshot_idx: 8, start_chunk: 730_750_000 }, // 20250915 2025-09-13T09:17:43Z
    ReplicaDayIndexEntry { snapshot_idx: 8, start_chunk: 731_850_000 }, // 20250916 2025-09-13T09:17:43Z
    ReplicaDayIndexEntry { snapshot_idx: 8, start_chunk: 732_940_000 }, // 20250917 2025-09-13T09:17:43Z
    ReplicaDayIndexEntry { snapshot_idx: 8, start_chunk: 734_030_000 }, // 20250918 2025-09-13T09:17:43Z
    ReplicaDayIndexEntry { snapshot_idx: 8, start_chunk: 735_110_000 }, // 20250919 2025-09-13T09:17:43Z
    ReplicaDayIndexEntry { snapshot_idx: 8, start_chunk: 736_200_000 }, // 20250920 2025-09-13T09:17:43Z
    ReplicaDayIndexEntry { snapshot_idx: 9, start_chunk: 737_563_000 }, // 20250921 2025-09-21T06:34:21Z
    ReplicaDayIndexEntry { snapshot_idx: 9, start_chunk: 738_360_000 }, // 20250922 2025-09-21T06:34:21Z
    ReplicaDayIndexEntry { snapshot_idx: 9, start_chunk: 739_440_000 }, // 20250923 2025-09-21T06:34:21Z
    ReplicaDayIndexEntry { snapshot_idx: 9, start_chunk: 740_530_000 }, // 20250924 2025-09-21T06:34:21Z
    ReplicaDayIndexEntry { snapshot_idx: 9, start_chunk: 741_620_000 }, // 20250925 2025-09-21T06:34:21Z
    ReplicaDayIndexEntry { snapshot_idx: 9, start_chunk: 742_700_000 }, // 20250926 2025-09-21T06:34:21Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 744_217_000 }, // 20250927 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 744_890_000 }, // 20250928 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 745_990_000 }, // 20250929 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 747_090_000 }, // 20250930 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 748_180_000 }, // 20251001 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 749_260_000 }, // 20251002 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 750_350_000 }, // 20251003 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 751_420_000 }, // 20251004 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 752_500_000 }, // 20251005 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 753_580_000 }, // 20251006 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 754_650_000 }, // 20251007 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 755_720_000 }, // 20251008 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 756_790_000 }, // 20251009 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 757_870_000 }, // 20251010 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 10, start_chunk: 758_930_000 }, // 20251011 2025-09-27T09:28:27Z
    ReplicaDayIndexEntry { snapshot_idx: 11, start_chunk: 760_363_000 }, // 20251012 2025-10-12T08:02:27Z
    ReplicaDayIndexEntry { snapshot_idx: 12, start_chunk: 761_532_000 }, // 20251013 2025-10-13T10:03:09Z
    ReplicaDayIndexEntry { snapshot_idx: 12, start_chunk: 762_170_000 }, // 20251014 2025-10-13T10:03:09Z
    ReplicaDayIndexEntry { snapshot_idx: 12, start_chunk: 763_240_000 }, // 20251015 2025-10-13T10:03:09Z
    ReplicaDayIndexEntry { snapshot_idx: 12, start_chunk: 764_310_000 }, // 20251016 2025-10-13T10:03:09Z
    ReplicaDayIndexEntry { snapshot_idx: 12, start_chunk: 765_380_000 }, // 20251017 2025-10-13T10:03:09Z
    ReplicaDayIndexEntry { snapshot_idx: 13, start_chunk: 766_823_000 }, // 20251018 2025-10-18T08:30:59Z
    ReplicaDayIndexEntry { snapshot_idx: 13, start_chunk: 767_530_000 }, // 20251019 2025-10-18T08:30:59Z
    ReplicaDayIndexEntry { snapshot_idx: 13, start_chunk: 768_620_000 }, // 20251020 2025-10-18T08:30:59Z
    ReplicaDayIndexEntry { snapshot_idx: 13, start_chunk: 769_700_000 }, // 20251021 2025-10-18T08:30:59Z
    ReplicaDayIndexEntry { snapshot_idx: 13, start_chunk: 770_780_000 }, // 20251022 2025-10-18T08:30:59Z
    ReplicaDayIndexEntry { snapshot_idx: 13, start_chunk: 771_870_000 }, // 20251023 2025-10-18T08:30:59Z
    ReplicaDayIndexEntry { snapshot_idx: 13, start_chunk: 772_960_000 }, // 20251024 2025-10-18T08:30:59Z
    ReplicaDayIndexEntry { snapshot_idx: 14, start_chunk: 774_399_000 }, // 20251025 2025-10-25T08:03:24Z
    ReplicaDayIndexEntry { snapshot_idx: 14, start_chunk: 775_140_000 }, // 20251026 2025-10-25T08:03:24Z
    ReplicaDayIndexEntry { snapshot_idx: 14, start_chunk: 776_240_000 }, // 20251027 2025-10-25T08:03:24Z
    ReplicaDayIndexEntry { snapshot_idx: 14, start_chunk: 777_340_000 }, // 20251028 2025-10-25T08:03:24Z
    ReplicaDayIndexEntry { snapshot_idx: 14, start_chunk: 778_430_000 }, // 20251029 2025-10-25T08:03:24Z
    ReplicaDayIndexEntry { snapshot_idx: 14, start_chunk: 779_520_000 }, // 20251030 2025-10-25T08:03:24Z
    ReplicaDayIndexEntry { snapshot_idx: 14, start_chunk: 780_610_000 }, // 20251031 2025-10-25T08:03:24Z
    ReplicaDayIndexEntry { snapshot_idx: 15, start_chunk: 782_049_000 }, // 20251101 2025-11-01T07:32:59Z
    ReplicaDayIndexEntry { snapshot_idx: 15, start_chunk: 782_810_000 }, // 20251102 2025-11-01T07:32:59Z
    ReplicaDayIndexEntry { snapshot_idx: 15, start_chunk: 783_900_000 }, // 20251103 2025-11-01T07:32:59Z
    ReplicaDayIndexEntry { snapshot_idx: 15, start_chunk: 784_990_000 }, // 20251104 2025-11-01T07:32:59Z
    ReplicaDayIndexEntry { snapshot_idx: 15, start_chunk: 786_080_000 }, // 20251105 2025-11-01T07:32:59Z
    ReplicaDayIndexEntry { snapshot_idx: 15, start_chunk: 787_180_000 }, // 20251106 2025-11-01T07:32:59Z
    ReplicaDayIndexEntry { snapshot_idx: 15, start_chunk: 788_270_000 }, // 20251107 2025-11-01T07:32:59Z
    ReplicaDayIndexEntry { snapshot_idx: 16, start_chunk: 789_733_000 }, // 20251108 2025-11-08T08:12:47Z
    ReplicaDayIndexEntry { snapshot_idx: 16, start_chunk: 790_440_000 }, // 20251109 2025-11-08T08:12:47Z
    ReplicaDayIndexEntry { snapshot_idx: 16, start_chunk: 791_510_000 }, // 20251110 2025-11-08T08:12:47Z
    ReplicaDayIndexEntry { snapshot_idx: 16, start_chunk: 792_580_000 }, // 20251111 2025-11-08T08:12:47Z
    ReplicaDayIndexEntry { snapshot_idx: 16, start_chunk: 793_640_000 }, // 20251112 2025-11-08T08:12:47Z
    ReplicaDayIndexEntry { snapshot_idx: 16, start_chunk: 794_700_000 }, // 20251113 2025-11-08T08:12:47Z
    ReplicaDayIndexEntry { snapshot_idx: 16, start_chunk: 795_770_000 }, // 20251114 2025-11-08T08:12:47Z
    ReplicaDayIndexEntry { snapshot_idx: 17, start_chunk: 797_181_000 }, // 20251115 2025-11-15T08:06:02Z
    ReplicaDayIndexEntry { snapshot_idx: 17, start_chunk: 797_890_000 }, // 20251116 2025-11-15T08:06:02Z
    ReplicaDayIndexEntry { snapshot_idx: 17, start_chunk: 798_950_000 }, // 20251117 2025-11-15T08:06:02Z
    ReplicaDayIndexEntry { snapshot_idx: 17, start_chunk: 800_010_000 }, // 20251118 2025-11-15T08:06:02Z
    ReplicaDayIndexEntry { snapshot_idx: 17, start_chunk: 801_080_000 }, // 20251119 2025-11-15T08:06:02Z
    ReplicaDayIndexEntry { snapshot_idx: 17, start_chunk: 802_130_000 }, // 20251120 2025-11-15T08:06:02Z
    ReplicaDayIndexEntry { snapshot_idx: 17, start_chunk: 803_190_000 }, // 20251121 2025-11-15T08:06:02Z
    ReplicaDayIndexEntry { snapshot_idx: 18, start_chunk: 804_679_000 }, // 20251122 2025-11-22T09:21:51Z
    ReplicaDayIndexEntry { snapshot_idx: 18, start_chunk: 805_340_000 }, // 20251123 2025-11-22T09:21:51Z
    ReplicaDayIndexEntry { snapshot_idx: 18, start_chunk: 806_400_000 }, // 20251124 2025-11-22T09:21:51Z
    ReplicaDayIndexEntry { snapshot_idx: 18, start_chunk: 807_470_000 }, // 20251125 2025-11-22T09:21:51Z
    ReplicaDayIndexEntry { snapshot_idx: 18, start_chunk: 808_540_000 }, // 20251126 2025-11-22T09:21:51Z
    ReplicaDayIndexEntry { snapshot_idx: 18, start_chunk: 809_610_000 }, // 20251127 2025-11-22T09:21:51Z
    ReplicaDayIndexEntry { snapshot_idx: 18, start_chunk: 810_670_000 }, // 20251128 2025-11-22T09:21:51Z
    ReplicaDayIndexEntry { snapshot_idx: 19, start_chunk: 812_129_000 }, // 20251129 2025-11-29T08:48:22Z
    ReplicaDayIndexEntry { snapshot_idx: 19, start_chunk: 812_810_000 }, // 20251130 2025-11-29T08:48:22Z
    ReplicaDayIndexEntry { snapshot_idx: 19, start_chunk: 813_870_000 }, // 20251201 2025-11-29T08:48:22Z
    ReplicaDayIndexEntry { snapshot_idx: 19, start_chunk: 814_950_000 }, // 20251202 2025-11-29T08:48:22Z
    ReplicaDayIndexEntry { snapshot_idx: 19, start_chunk: 816_020_000 }, // 20251203 2025-11-29T08:48:22Z
    ReplicaDayIndexEntry { snapshot_idx: 19, start_chunk: 817_080_000 }, // 20251204 2025-11-29T08:48:22Z
    ReplicaDayIndexEntry { snapshot_idx: 19, start_chunk: 818_140_000 }, // 20251205 2025-11-29T08:48:22Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 819_541_000 }, // 20251206 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 820_240_000 }, // 20251207 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 821_310_000 }, // 20251208 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 822_380_000 }, // 20251209 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 823_450_000 }, // 20251210 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 824_520_000 }, // 20251211 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 825_590_000 }, // 20251212 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 826_660_000 }, // 20251213 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 827_740_000 }, // 20251214 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 828_820_000 }, // 20251215 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 829_890_000 }, // 20251216 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 830_950_000 }, // 20251217 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 832_030_000 }, // 20251218 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 20, start_chunk: 833_100_000 }, // 20251219 2025-12-06T08:07:53Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 834_515_000 }, // 20251220 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 835_250_000 }, // 20251221 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 836_310_000 }, // 20251222 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 837_370_000 }, // 20251223 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 838_430_000 }, // 20251224 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 839_490_000 }, // 20251225 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 840_550_000 }, // 20251226 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 841_610_000 }, // 20251227 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 842_670_000 }, // 20251228 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 843_730_000 }, // 20251229 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 844_790_000 }, // 20251230 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 845_850_000 }, // 20251231 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 846_910_000 }, // 20260101 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 21, start_chunk: 847_970_000 }, // 20260102 2025-12-20T07:47:44Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 849_331_000 }, // 20260103 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 850_080_000 }, // 20260104 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 851_130_000 }, // 20260105 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 852_190_000 }, // 20260106 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 853_250_000 }, // 20260107 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 854_310_000 }, // 20260108 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 855_360_000 }, // 20260109 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 856_410_000 }, // 20260110 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 857_460_000 }, // 20260111 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 858_500_000 }, // 20260112 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 859_560_000 }, // 20260113 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 860_610_000 }, // 20260114 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 861_670_000 }, // 20260115 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 22, start_chunk: 862_720_000 }, // 20260116 2026-01-03T07:05:33Z
    ReplicaDayIndexEntry { snapshot_idx: 23, start_chunk: 864_091_000 }, // 20260117 2026-01-17T07:05:45Z
    ReplicaDayIndexEntry { snapshot_idx: 23, start_chunk: 864_840_000 }, // 20260118 2026-01-17T07:05:45Z
    ReplicaDayIndexEntry { snapshot_idx: 23, start_chunk: 865_890_000 }, // 20260119 2026-01-17T07:05:45Z
    ReplicaDayIndexEntry { snapshot_idx: 23, start_chunk: 866_950_000 }, // 20260120 2026-01-17T07:05:45Z
    ReplicaDayIndexEntry { snapshot_idx: 23, start_chunk: 868_000_000 }, // 20260121 2026-01-17T07:05:45Z
    ReplicaDayIndexEntry { snapshot_idx: 23, start_chunk: 869_060_000 }, // 20260122 2026-01-17T07:05:45Z
    ReplicaDayIndexEntry { snapshot_idx: 23, start_chunk: 870_100_000 }, // 20260123 2026-01-17T07:05:45Z
    ReplicaDayIndexEntry { snapshot_idx: 24, start_chunk: 871_512_000 }, // 20260124 2026-01-24T08:02:26Z
    ReplicaDayIndexEntry { snapshot_idx: 24, start_chunk: 872_220_000 }, // 20260125 2026-01-24T08:02:26Z
    ReplicaDayIndexEntry { snapshot_idx: 24, start_chunk: 873_280_000 }, // 20260126 2026-01-24T08:02:26Z
    ReplicaDayIndexEntry { snapshot_idx: 24, start_chunk: 874_340_000 }, // 20260127 2026-01-24T08:02:26Z
    ReplicaDayIndexEntry { snapshot_idx: 24, start_chunk: 875_430_000 }, // 20260128 2026-01-24T08:02:26Z
    ReplicaDayIndexEntry { snapshot_idx: 24, start_chunk: 876_510_000 }, // 20260129 2026-01-24T08:02:26Z
    ReplicaDayIndexEntry { snapshot_idx: 24, start_chunk: 877_590_000 }, // 20260130 2026-01-24T08:02:26Z
    ReplicaDayIndexEntry { snapshot_idx: 25, start_chunk: 879_022_000 }, // 20260131 2026-01-31T08:04:34Z
    ReplicaDayIndexEntry { snapshot_idx: 25, start_chunk: 879_740_000 }, // 20260201 2026-01-31T08:04:34Z
    ReplicaDayIndexEntry { snapshot_idx: 25, start_chunk: 880_810_000 }, // 20260202 2026-01-31T08:04:34Z
    ReplicaDayIndexEntry { snapshot_idx: 25, start_chunk: 881_880_000 }, // 20260203 2026-01-31T08:04:34Z
    ReplicaDayIndexEntry { snapshot_idx: 25, start_chunk: 882_960_000 }, // 20260204 2026-01-31T08:04:34Z
    ReplicaDayIndexEntry { snapshot_idx: 25, start_chunk: 884_030_000 }, // 20260205 2026-01-31T08:04:34Z
    ReplicaDayIndexEntry { snapshot_idx: 25, start_chunk: 885_090_000 }, // 20260206 2026-01-31T08:04:34Z
    ReplicaDayIndexEntry { snapshot_idx: 26, start_chunk: 886_461_000 }, // 20260207 2026-02-07T07:04:22Z
    ReplicaDayIndexEntry { snapshot_idx: 26, start_chunk: 887_210_000 }, // 20260208 2026-02-07T07:04:22Z
    ReplicaDayIndexEntry { snapshot_idx: 26, start_chunk: 888_260_000 }, // 20260209 2026-02-07T07:04:22Z
    ReplicaDayIndexEntry { snapshot_idx: 26, start_chunk: 889_310_000 }, // 20260210 2026-02-07T07:04:22Z
    ReplicaDayIndexEntry { snapshot_idx: 26, start_chunk: 890_360_000 }, // 20260211 2026-02-07T07:04:22Z
    ReplicaDayIndexEntry { snapshot_idx: 26, start_chunk: 891_410_000 }, // 20260212 2026-02-07T07:04:22Z
    ReplicaDayIndexEntry { snapshot_idx: 26, start_chunk: 892_460_000 }, // 20260213 2026-02-07T07:04:22Z
    ReplicaDayIndexEntry { snapshot_idx: 26, start_chunk: 893_510_000 }, // 20260214 2026-02-07T07:04:22Z
    ReplicaDayIndexEntry { snapshot_idx: 26, start_chunk: 894_550_000 }, // 20260215 2026-02-07T07:04:22Z
    ReplicaDayIndexEntry { snapshot_idx: 27, start_chunk: 896_135_000 }, // 20260216 2026-02-16T12:21:56Z
    ReplicaDayIndexEntry { snapshot_idx: 27, start_chunk: 896_650_000 }, // 20260217 2026-02-16T12:21:56Z
    ReplicaDayIndexEntry { snapshot_idx: 27, start_chunk: 897_700_000 }, // 20260218 2026-02-16T12:21:56Z
    ReplicaDayIndexEntry { snapshot_idx: 27, start_chunk: 898_750_000 }, // 20260219 2026-02-16T12:21:56Z
    ReplicaDayIndexEntry { snapshot_idx: 27, start_chunk: 899_800_000 }, // 20260220 2026-02-16T12:21:56Z
    ReplicaDayIndexEntry { snapshot_idx: 28, start_chunk: 901_177_000 }, // 20260221 2026-02-21T07:34:29Z
    ReplicaDayIndexEntry { snapshot_idx: 28, start_chunk: 901_910_000 }, // 20260222 2026-02-21T07:34:29Z
    ReplicaDayIndexEntry { snapshot_idx: 28, start_chunk: 902_960_000 }, // 20260223 2026-02-21T07:34:29Z
    ReplicaDayIndexEntry { snapshot_idx: 28, start_chunk: 904_030_000 }, // 20260224 2026-02-21T07:34:29Z
    ReplicaDayIndexEntry { snapshot_idx: 28, start_chunk: 905_080_000 }, // 20260225 2026-02-21T07:34:29Z
    ReplicaDayIndexEntry { snapshot_idx: 28, start_chunk: 906_140_000 }, // 20260226 2026-02-21T07:34:29Z
    ReplicaDayIndexEntry { snapshot_idx: 28, start_chunk: 907_200_000 }, // 20260227 2026-02-21T07:34:29Z
    ReplicaDayIndexEntry { snapshot_idx: 28, start_chunk: 908_250_000 }, // 20260228 2026-02-21T07:34:29Z
    ReplicaDayIndexEntry { snapshot_idx: 29, start_chunk: 909_615_000 }, // 20260301 2026-03-01T07:06:51Z
    ReplicaDayIndexEntry { snapshot_idx: 29, start_chunk: 910_370_000 }, // 20260302 2026-03-01T07:06:51Z
    ReplicaDayIndexEntry { snapshot_idx: 29, start_chunk: 911_430_000 }, // 20260303 2026-03-01T07:06:51Z
    ReplicaDayIndexEntry { snapshot_idx: 29, start_chunk: 912_500_000 }, // 20260304 2026-03-01T07:06:51Z
    ReplicaDayIndexEntry { snapshot_idx: 29, start_chunk: 913_550_000 }, // 20260305 2026-03-01T07:06:51Z
    ReplicaDayIndexEntry { snapshot_idx: 29, start_chunk: 914_590_000 }, // 20260306 2026-03-01T07:06:51Z
    ReplicaDayIndexEntry { snapshot_idx: 29, start_chunk: 915_630_000 }, // 20260307 2026-03-01T07:06:51Z
    ReplicaDayIndexEntry { snapshot_idx: 29, start_chunk: 916_680_000 }, // 20260308 2026-03-01T07:06:51Z
    ReplicaDayIndexEntry { snapshot_idx: 29, start_chunk: 917_720_000 }, // 20260309 2026-03-01T07:06:51Z
    ReplicaDayIndexEntry { snapshot_idx: 29, start_chunk: 918_760_000 }, // 20260310 2026-03-01T07:06:51Z
    ReplicaDayIndexEntry { snapshot_idx: 29, start_chunk: 919_800_000 }, // 20260311 2026-03-01T07:06:51Z
    ReplicaDayIndexEntry { snapshot_idx: 29, start_chunk: 920_840_000 }, // 20260312 2026-03-01T07:06:51Z
    ReplicaDayIndexEntry { snapshot_idx: 29, start_chunk: 921_880_000 }, // 20260313 2026-03-01T07:06:51Z
    ReplicaDayIndexEntry { snapshot_idx: 30, start_chunk: 923_212_000 }, // 20260314 2026-03-14T07:05:59Z
    ReplicaDayIndexEntry { snapshot_idx: 30, start_chunk: 923_960_000 }, // 20260315 2026-03-14T07:05:59Z
    ReplicaDayIndexEntry { snapshot_idx: 30, start_chunk: 924_990_000 }, // 20260316 2026-03-14T07:05:59Z
    ReplicaDayIndexEntry { snapshot_idx: 30, start_chunk: 926_030_000 }, // 20260317 2026-03-14T07:05:59Z
    ReplicaDayIndexEntry { snapshot_idx: 30, start_chunk: 927_070_000 }, // 20260318 2026-03-14T07:05:59Z
    ReplicaDayIndexEntry { snapshot_idx: 30, start_chunk: 928_110_000 }, // 20260319 2026-03-14T07:05:59Z
    ReplicaDayIndexEntry { snapshot_idx: 30, start_chunk: 929_160_000 }, // 20260320 2026-03-14T07:05:59Z
    ReplicaDayIndexEntry { snapshot_idx: 31, start_chunk: 930_593_000 }, // 20260321 2026-03-21T09:33:29Z
    ReplicaDayIndexEntry { snapshot_idx: 31, start_chunk: 931_230_000 }, // 20260322 2026-03-21T09:33:29Z
    ReplicaDayIndexEntry { snapshot_idx: 31, start_chunk: 932_270_000 }, // 20260323 2026-03-21T09:33:29Z
    ReplicaDayIndexEntry { snapshot_idx: 31, start_chunk: 933_300_000 }, // 20260324 2026-03-21T09:33:29Z
    ReplicaDayIndexEntry { snapshot_idx: 31, start_chunk: 934_330_000 }, // 20260325 2026-03-21T09:33:29Z
    ReplicaDayIndexEntry { snapshot_idx: 31, start_chunk: 935_370_000 }, // 20260326 2026-03-21T09:33:29Z
    ReplicaDayIndexEntry { snapshot_idx: 31, start_chunk: 936_420_000 }, // 20260327 2026-03-21T09:33:29Z
    ReplicaDayIndexEntry { snapshot_idx: 31, start_chunk: 937_470_000 }, // 20260328 2026-03-21T09:33:29Z
    ReplicaDayIndexEntry { snapshot_idx: 31, start_chunk: 938_510_000 }, // 20260329 2026-03-21T09:33:29Z
    ReplicaDayIndexEntry { snapshot_idx: 31, start_chunk: 939_560_000 }, // 20260330 2026-03-21T09:33:29Z
    ReplicaDayIndexEntry { snapshot_idx: 31, start_chunk: 940_610_000 }, // 20260331 2026-03-21T09:33:29Z
];
