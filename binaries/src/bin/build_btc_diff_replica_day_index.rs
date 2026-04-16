// Auto-generated from s3://hl-mainnet-node-data/replica_cmds (requester pays).
// Coverage: 20250720..20260415.
// This is a chunk-segment index, not a one-entry-per-day index.

pub(crate) const REPLICA_INDEX_END_EXCLUSIVE_CHUNK: u64 = 960_290_000; // first chunk after 20260415

pub(crate) const REPLICA_SNAPSHOT_DIRS: [&str; 35] = [
    "2025-07-19T10:13:42Z",
    "2025-07-27T08:48:35Z",
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
    "2026-04-04T07:35:15Z",
    "2026-04-13T08:41:11Z",
];

#[derive(Clone, Copy, Debug)]
pub(crate) struct ReplicaChunkIndexEntry {
    pub(crate) snapshot_idx: u8,
    pub(crate) date_yyyymmdd: u32,
    pub(crate) start_chunk: u64,
}

pub(crate) const REPLICA_CHUNK_INDEX: [ReplicaChunkIndexEntry; 338] = [
    ReplicaChunkIndexEntry { snapshot_idx: 0, date_yyyymmdd: 20250720, start_chunk: 668_430_000 }, // 20250720 2025-07-19T10:13:42Z end=669_540_000 count=112
    ReplicaChunkIndexEntry { snapshot_idx: 0, date_yyyymmdd: 20250721, start_chunk: 669_550_000 }, // 20250721 2025-07-19T10:13:42Z end=670_660_000 count=112
    ReplicaChunkIndexEntry { snapshot_idx: 0, date_yyyymmdd: 20250722, start_chunk: 670_670_000 }, // 20250722 2025-07-19T10:13:42Z end=671_760_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 0, date_yyyymmdd: 20250723, start_chunk: 671_770_000 }, // 20250723 2025-07-19T10:13:42Z end=672_870_000 count=111
    ReplicaChunkIndexEntry { snapshot_idx: 0, date_yyyymmdd: 20250724, start_chunk: 672_880_000 }, // 20250724 2025-07-19T10:13:42Z end=673_960_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 0, date_yyyymmdd: 20250725, start_chunk: 673_970_000 }, // 20250725 2025-07-19T10:13:42Z end=675_060_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 0, date_yyyymmdd: 20250726, start_chunk: 675_070_000 }, // 20250726 2025-07-19T10:13:42Z end=676_190_000 count=113
    ReplicaChunkIndexEntry { snapshot_idx: 0, date_yyyymmdd: 20250727, start_chunk: 676_200_000 }, // 20250727 2025-07-19T10:13:42Z end=676_600_000 count=41
    ReplicaChunkIndexEntry { snapshot_idx: 1, date_yyyymmdd: 20250727, start_chunk: 676_607_000 }, // 20250727 2025-07-27T08:48:35Z end=676_607_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 1, date_yyyymmdd: 20250727, start_chunk: 676_610_000 }, // 20250727 2025-07-27T08:48:35Z end=676_710_000 count=11
    ReplicaChunkIndexEntry { snapshot_idx: 2, date_yyyymmdd: 20250727, start_chunk: 676_714_000 }, // 20250727 2025-07-27T12:00:27Z end=676_714_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 2, date_yyyymmdd: 20250727, start_chunk: 676_720_000 }, // 20250727 2025-07-27T12:00:27Z end=677_270_000 count=56
    ReplicaChunkIndexEntry { snapshot_idx: 2, date_yyyymmdd: 20250728, start_chunk: 677_280_000 }, // 20250728 2025-07-27T12:00:27Z end=678_370_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 2, date_yyyymmdd: 20250729, start_chunk: 678_380_000 }, // 20250729 2025-07-27T12:00:27Z end=679_460_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 2, date_yyyymmdd: 20250730, start_chunk: 679_470_000 }, // 20250730 2025-07-27T12:00:27Z end=680_550_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 2, date_yyyymmdd: 20250731, start_chunk: 680_560_000 }, // 20250731 2025-07-27T12:00:27Z end=681_640_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 2, date_yyyymmdd: 20250801, start_chunk: 681_650_000 }, // 20250801 2025-07-27T12:00:27Z end=682_710_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 2, date_yyyymmdd: 20250802, start_chunk: 682_720_000 }, // 20250802 2025-07-27T12:00:27Z end=683_140_000 count=43
    ReplicaChunkIndexEntry { snapshot_idx: 3, date_yyyymmdd: 20250802, start_chunk: 683_143_000 }, // 20250802 2025-08-02T09:18:00Z end=683_143_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 3, date_yyyymmdd: 20250802, start_chunk: 683_150_000 }, // 20250802 2025-08-02T09:18:00Z end=683_800_000 count=66
    ReplicaChunkIndexEntry { snapshot_idx: 3, date_yyyymmdd: 20250803, start_chunk: 683_810_000 }, // 20250803 2025-08-02T09:18:00Z end=684_910_000 count=111
    ReplicaChunkIndexEntry { snapshot_idx: 3, date_yyyymmdd: 20250804, start_chunk: 684_920_000 }, // 20250804 2025-08-02T09:18:00Z end=686_020_000 count=111
    ReplicaChunkIndexEntry { snapshot_idx: 3, date_yyyymmdd: 20250805, start_chunk: 686_030_000 }, // 20250805 2025-08-02T09:18:00Z end=687_130_000 count=111
    ReplicaChunkIndexEntry { snapshot_idx: 3, date_yyyymmdd: 20250806, start_chunk: 687_140_000 }, // 20250806 2025-08-02T09:18:00Z end=688_220_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 3, date_yyyymmdd: 20250807, start_chunk: 688_230_000 }, // 20250807 2025-08-02T09:18:00Z end=689_330_000 count=111
    ReplicaChunkIndexEntry { snapshot_idx: 3, date_yyyymmdd: 20250808, start_chunk: 689_340_000 }, // 20250808 2025-08-02T09:18:00Z end=690_440_000 count=111
    ReplicaChunkIndexEntry { snapshot_idx: 3, date_yyyymmdd: 20250809, start_chunk: 690_450_000 }, // 20250809 2025-08-02T09:18:00Z end=690_820_000 count=38
    ReplicaChunkIndexEntry { snapshot_idx: 4, date_yyyymmdd: 20250809, start_chunk: 690_826_000 }, // 20250809 2025-08-09T08:16:44Z end=690_826_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 4, date_yyyymmdd: 20250809, start_chunk: 690_830_000 }, // 20250809 2025-08-09T08:16:44Z end=691_540_000 count=72
    ReplicaChunkIndexEntry { snapshot_idx: 4, date_yyyymmdd: 20250810, start_chunk: 691_550_000 }, // 20250810 2025-08-09T08:16:44Z end=692_640_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 4, date_yyyymmdd: 20250811, start_chunk: 692_650_000 }, // 20250811 2025-08-09T08:16:44Z end=693_730_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 4, date_yyyymmdd: 20250812, start_chunk: 693_740_000 }, // 20250812 2025-08-09T08:16:44Z end=694_820_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 4, date_yyyymmdd: 20250813, start_chunk: 694_830_000 }, // 20250813 2025-08-09T08:16:44Z end=695_900_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 4, date_yyyymmdd: 20250814, start_chunk: 695_910_000 }, // 20250814 2025-08-09T08:16:44Z end=696_960_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 4, date_yyyymmdd: 20250815, start_chunk: 696_970_000 }, // 20250815 2025-08-09T08:16:44Z end=698_050_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 4, date_yyyymmdd: 20250816, start_chunk: 698_060_000 }, // 20250816 2025-08-09T08:16:44Z end=698_440_000 count=39
    ReplicaChunkIndexEntry { snapshot_idx: 5, date_yyyymmdd: 20250816, start_chunk: 698_441_000 }, // 20250816 2025-08-16T08:22:05Z end=698_441_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 5, date_yyyymmdd: 20250816, start_chunk: 698_450_000 }, // 20250816 2025-08-16T08:22:05Z end=699_170_000 count=73
    ReplicaChunkIndexEntry { snapshot_idx: 5, date_yyyymmdd: 20250817, start_chunk: 699_180_000 }, // 20250817 2025-08-16T08:22:05Z end=700_290_000 count=112
    ReplicaChunkIndexEntry { snapshot_idx: 5, date_yyyymmdd: 20250818, start_chunk: 700_300_000 }, // 20250818 2025-08-16T08:22:05Z end=701_380_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 5, date_yyyymmdd: 20250819, start_chunk: 701_390_000 }, // 20250819 2025-08-16T08:22:05Z end=702_460_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 5, date_yyyymmdd: 20250820, start_chunk: 702_470_000 }, // 20250820 2025-08-16T08:22:05Z end=703_540_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 5, date_yyyymmdd: 20250821, start_chunk: 703_550_000 }, // 20250821 2025-08-16T08:22:05Z end=704_630_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 5, date_yyyymmdd: 20250822, start_chunk: 704_640_000 }, // 20250822 2025-08-16T08:22:05Z end=705_710_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 5, date_yyyymmdd: 20250823, start_chunk: 705_720_000 }, // 20250823 2025-08-16T08:22:05Z end=706_110_000 count=40
    ReplicaChunkIndexEntry { snapshot_idx: 6, date_yyyymmdd: 20250823, start_chunk: 706_113_000 }, // 20250823 2025-08-23T08:56:46Z end=706_113_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 6, date_yyyymmdd: 20250823, start_chunk: 706_120_000 }, // 20250823 2025-08-23T08:56:46Z end=706_800_000 count=69
    ReplicaChunkIndexEntry { snapshot_idx: 6, date_yyyymmdd: 20250824, start_chunk: 706_810_000 }, // 20250824 2025-08-23T08:56:46Z end=707_910_000 count=111
    ReplicaChunkIndexEntry { snapshot_idx: 6, date_yyyymmdd: 20250825, start_chunk: 707_920_000 }, // 20250825 2025-08-23T08:56:46Z end=709_000_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 6, date_yyyymmdd: 20250826, start_chunk: 709_010_000 }, // 20250826 2025-08-23T08:56:46Z end=710_100_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 6, date_yyyymmdd: 20250827, start_chunk: 710_110_000 }, // 20250827 2025-08-23T08:56:46Z end=711_200_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 6, date_yyyymmdd: 20250828, start_chunk: 711_210_000 }, // 20250828 2025-08-23T08:56:46Z end=712_300_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 6, date_yyyymmdd: 20250829, start_chunk: 712_310_000 }, // 20250829 2025-08-23T08:56:46Z end=713_380_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 6, date_yyyymmdd: 20250830, start_chunk: 713_390_000 }, // 20250830 2025-08-23T08:56:46Z end=713_770_000 count=39
    ReplicaChunkIndexEntry { snapshot_idx: 7, date_yyyymmdd: 20250830, start_chunk: 713_771_000 }, // 20250830 2025-08-30T08:20:04Z end=713_771_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 7, date_yyyymmdd: 20250830, start_chunk: 713_780_000 }, // 20250830 2025-08-30T08:20:04Z end=714_480_000 count=71
    ReplicaChunkIndexEntry { snapshot_idx: 7, date_yyyymmdd: 20250831, start_chunk: 714_490_000 }, // 20250831 2025-08-30T08:20:04Z end=715_580_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 7, date_yyyymmdd: 20250901, start_chunk: 715_590_000 }, // 20250901 2025-08-30T08:20:04Z end=716_650_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 7, date_yyyymmdd: 20250902, start_chunk: 716_660_000 }, // 20250902 2025-08-30T08:20:04Z end=717_730_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 7, date_yyyymmdd: 20250903, start_chunk: 717_740_000 }, // 20250903 2025-08-30T08:20:04Z end=718_810_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 7, date_yyyymmdd: 20250904, start_chunk: 718_820_000 }, // 20250904 2025-08-30T08:20:04Z end=719_890_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 7, date_yyyymmdd: 20250905, start_chunk: 719_900_000 }, // 20250905 2025-08-30T08:20:04Z end=720_960_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 7, date_yyyymmdd: 20250906, start_chunk: 720_970_000 }, // 20250906 2025-08-30T08:20:04Z end=721_340_000 count=38
    ReplicaChunkIndexEntry { snapshot_idx: 8, date_yyyymmdd: 20250906, start_chunk: 721_349_000 }, // 20250906 2025-09-06T08:31:30Z end=721_349_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 8, date_yyyymmdd: 20250906, start_chunk: 721_350_000 }, // 20250906 2025-09-06T08:31:30Z end=722_040_000 count=70
    ReplicaChunkIndexEntry { snapshot_idx: 8, date_yyyymmdd: 20250907, start_chunk: 722_050_000 }, // 20250907 2025-09-06T08:31:30Z end=723_130_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 8, date_yyyymmdd: 20250908, start_chunk: 723_140_000 }, // 20250908 2025-09-06T08:31:30Z end=724_220_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 8, date_yyyymmdd: 20250909, start_chunk: 724_230_000 }, // 20250909 2025-09-06T08:31:30Z end=725_290_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 8, date_yyyymmdd: 20250910, start_chunk: 725_300_000 }, // 20250910 2025-09-06T08:31:30Z end=726_380_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 8, date_yyyymmdd: 20250911, start_chunk: 726_390_000 }, // 20250911 2025-09-06T08:31:30Z end=727_470_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 8, date_yyyymmdd: 20250912, start_chunk: 727_480_000 }, // 20250912 2025-09-06T08:31:30Z end=728_560_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 8, date_yyyymmdd: 20250913, start_chunk: 728_570_000 }, // 20250913 2025-09-06T08:31:30Z end=728_980_000 count=42
    ReplicaChunkIndexEntry { snapshot_idx: 9, date_yyyymmdd: 20250913, start_chunk: 728_982_000 }, // 20250913 2025-09-13T09:17:43Z end=728_982_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 9, date_yyyymmdd: 20250913, start_chunk: 728_990_000 }, // 20250913 2025-09-13T09:17:43Z end=729_650_000 count=67
    ReplicaChunkIndexEntry { snapshot_idx: 9, date_yyyymmdd: 20250914, start_chunk: 729_660_000 }, // 20250914 2025-09-13T09:17:43Z end=730_740_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 9, date_yyyymmdd: 20250915, start_chunk: 730_750_000 }, // 20250915 2025-09-13T09:17:43Z end=731_840_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 9, date_yyyymmdd: 20250916, start_chunk: 731_850_000 }, // 20250916 2025-09-13T09:17:43Z end=732_930_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 9, date_yyyymmdd: 20250917, start_chunk: 732_940_000 }, // 20250917 2025-09-13T09:17:43Z end=734_020_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 9, date_yyyymmdd: 20250918, start_chunk: 734_030_000 }, // 20250918 2025-09-13T09:17:43Z end=735_100_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 9, date_yyyymmdd: 20250919, start_chunk: 735_110_000 }, // 20250919 2025-09-13T09:17:43Z end=736_190_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 9, date_yyyymmdd: 20250920, start_chunk: 736_200_000 }, // 20250920 2025-09-13T09:17:43Z end=737_260_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 9, date_yyyymmdd: 20250921, start_chunk: 737_270_000 }, // 20250921 2025-09-13T09:17:43Z end=737_560_000 count=30
    ReplicaChunkIndexEntry { snapshot_idx: 10, date_yyyymmdd: 20250921, start_chunk: 737_563_000 }, // 20250921 2025-09-21T06:34:21Z end=737_563_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 10, date_yyyymmdd: 20250921, start_chunk: 737_570_000 }, // 20250921 2025-09-21T06:34:21Z end=738_350_000 count=79
    ReplicaChunkIndexEntry { snapshot_idx: 10, date_yyyymmdd: 20250922, start_chunk: 738_360_000 }, // 20250922 2025-09-21T06:34:21Z end=739_430_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 10, date_yyyymmdd: 20250923, start_chunk: 739_440_000 }, // 20250923 2025-09-21T06:34:21Z end=740_520_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 10, date_yyyymmdd: 20250924, start_chunk: 740_530_000 }, // 20250924 2025-09-21T06:34:21Z end=741_610_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 10, date_yyyymmdd: 20250925, start_chunk: 741_620_000 }, // 20250925 2025-09-21T06:34:21Z end=742_690_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 10, date_yyyymmdd: 20250926, start_chunk: 742_700_000 }, // 20250926 2025-09-21T06:34:21Z end=743_780_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 10, date_yyyymmdd: 20250927, start_chunk: 743_790_000 }, // 20250927 2025-09-21T06:34:21Z end=744_210_000 count=43
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20250927, start_chunk: 744_217_000 }, // 20250927 2025-09-27T09:28:27Z end=744_217_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20250927, start_chunk: 744_220_000 }, // 20250927 2025-09-27T09:28:27Z end=744_880_000 count=67
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20250928, start_chunk: 744_890_000 }, // 20250928 2025-09-27T09:28:27Z end=745_980_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20250929, start_chunk: 745_990_000 }, // 20250929 2025-09-27T09:28:27Z end=747_080_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20250930, start_chunk: 747_090_000 }, // 20250930 2025-09-27T09:28:27Z end=748_170_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20251001, start_chunk: 748_180_000 }, // 20251001 2025-09-27T09:28:27Z end=749_250_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20251002, start_chunk: 749_260_000 }, // 20251002 2025-09-27T09:28:27Z end=750_340_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20251003, start_chunk: 750_350_000 }, // 20251003 2025-09-27T09:28:27Z end=751_410_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20251004, start_chunk: 751_420_000 }, // 20251004 2025-09-27T09:28:27Z end=752_490_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20251005, start_chunk: 752_500_000 }, // 20251005 2025-09-27T09:28:27Z end=753_570_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20251006, start_chunk: 753_580_000 }, // 20251006 2025-09-27T09:28:27Z end=754_640_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20251007, start_chunk: 754_650_000 }, // 20251007 2025-09-27T09:28:27Z end=755_710_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20251008, start_chunk: 755_720_000 }, // 20251008 2025-09-27T09:28:27Z end=756_780_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20251009, start_chunk: 756_790_000 }, // 20251009 2025-09-27T09:28:27Z end=757_860_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20251010, start_chunk: 757_870_000 }, // 20251010 2025-09-27T09:28:27Z end=758_920_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20251011, start_chunk: 758_930_000 }, // 20251011 2025-09-27T09:28:27Z end=760_000_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 11, date_yyyymmdd: 20251012, start_chunk: 760_010_000 }, // 20251012 2025-09-27T09:28:27Z end=760_360_000 count=36
    ReplicaChunkIndexEntry { snapshot_idx: 12, date_yyyymmdd: 20251012, start_chunk: 760_363_000 }, // 20251012 2025-10-12T08:02:27Z end=760_363_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 12, date_yyyymmdd: 20251012, start_chunk: 760_370_000 }, // 20251012 2025-10-12T08:02:27Z end=761_080_000 count=72
    ReplicaChunkIndexEntry { snapshot_idx: 12, date_yyyymmdd: 20251013, start_chunk: 761_090_000 }, // 20251013 2025-10-12T08:02:27Z end=761_530_000 count=45
    ReplicaChunkIndexEntry { snapshot_idx: 13, date_yyyymmdd: 20251013, start_chunk: 761_532_000 }, // 20251013 2025-10-13T10:03:09Z end=761_532_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 13, date_yyyymmdd: 20251013, start_chunk: 761_540_000 }, // 20251013 2025-10-13T10:03:09Z end=762_160_000 count=63
    ReplicaChunkIndexEntry { snapshot_idx: 13, date_yyyymmdd: 20251014, start_chunk: 762_170_000 }, // 20251014 2025-10-13T10:03:09Z end=763_230_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 13, date_yyyymmdd: 20251015, start_chunk: 763_240_000 }, // 20251015 2025-10-13T10:03:09Z end=764_300_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 13, date_yyyymmdd: 20251016, start_chunk: 764_310_000 }, // 20251016 2025-10-13T10:03:09Z end=765_370_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 13, date_yyyymmdd: 20251017, start_chunk: 765_380_000 }, // 20251017 2025-10-13T10:03:09Z end=766_440_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 13, date_yyyymmdd: 20251018, start_chunk: 766_450_000 }, // 20251018 2025-10-13T10:03:09Z end=766_820_000 count=38
    ReplicaChunkIndexEntry { snapshot_idx: 14, date_yyyymmdd: 20251018, start_chunk: 766_823_000 }, // 20251018 2025-10-18T08:30:59Z end=766_823_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 14, date_yyyymmdd: 20251018, start_chunk: 766_830_000 }, // 20251018 2025-10-18T08:30:59Z end=767_520_000 count=70
    ReplicaChunkIndexEntry { snapshot_idx: 14, date_yyyymmdd: 20251019, start_chunk: 767_530_000 }, // 20251019 2025-10-18T08:30:59Z end=768_610_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 14, date_yyyymmdd: 20251020, start_chunk: 768_620_000 }, // 20251020 2025-10-18T08:30:59Z end=769_690_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 14, date_yyyymmdd: 20251021, start_chunk: 769_700_000 }, // 20251021 2025-10-18T08:30:59Z end=770_770_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 14, date_yyyymmdd: 20251022, start_chunk: 770_780_000 }, // 20251022 2025-10-18T08:30:59Z end=771_860_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 14, date_yyyymmdd: 20251023, start_chunk: 771_870_000 }, // 20251023 2025-10-18T08:30:59Z end=772_950_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 14, date_yyyymmdd: 20251024, start_chunk: 772_960_000 }, // 20251024 2025-10-18T08:30:59Z end=774_030_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 14, date_yyyymmdd: 20251025, start_chunk: 774_040_000 }, // 20251025 2025-10-18T08:30:59Z end=774_390_000 count=36
    ReplicaChunkIndexEntry { snapshot_idx: 15, date_yyyymmdd: 20251025, start_chunk: 774_399_000 }, // 20251025 2025-10-25T08:03:24Z end=774_399_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 15, date_yyyymmdd: 20251025, start_chunk: 774_400_000 }, // 20251025 2025-10-25T08:03:24Z end=775_130_000 count=74
    ReplicaChunkIndexEntry { snapshot_idx: 15, date_yyyymmdd: 20251026, start_chunk: 775_140_000 }, // 20251026 2025-10-25T08:03:24Z end=776_230_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 15, date_yyyymmdd: 20251027, start_chunk: 776_240_000 }, // 20251027 2025-10-25T08:03:24Z end=777_330_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 15, date_yyyymmdd: 20251028, start_chunk: 777_340_000 }, // 20251028 2025-10-25T08:03:24Z end=778_420_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 15, date_yyyymmdd: 20251029, start_chunk: 778_430_000 }, // 20251029 2025-10-25T08:03:24Z end=779_510_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 15, date_yyyymmdd: 20251030, start_chunk: 779_520_000 }, // 20251030 2025-10-25T08:03:24Z end=780_600_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 15, date_yyyymmdd: 20251031, start_chunk: 780_610_000 }, // 20251031 2025-10-25T08:03:24Z end=781_700_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 15, date_yyyymmdd: 20251101, start_chunk: 781_710_000 }, // 20251101 2025-10-25T08:03:24Z end=782_040_000 count=34
    ReplicaChunkIndexEntry { snapshot_idx: 16, date_yyyymmdd: 20251101, start_chunk: 782_049_000 }, // 20251101 2025-11-01T07:32:59Z end=782_049_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 16, date_yyyymmdd: 20251101, start_chunk: 782_050_000 }, // 20251101 2025-11-01T07:32:59Z end=782_800_000 count=76
    ReplicaChunkIndexEntry { snapshot_idx: 16, date_yyyymmdd: 20251102, start_chunk: 782_810_000 }, // 20251102 2025-11-01T07:32:59Z end=783_890_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 16, date_yyyymmdd: 20251103, start_chunk: 783_900_000 }, // 20251103 2025-11-01T07:32:59Z end=784_980_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 16, date_yyyymmdd: 20251104, start_chunk: 784_990_000 }, // 20251104 2025-11-01T07:32:59Z end=786_070_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 16, date_yyyymmdd: 20251105, start_chunk: 786_080_000 }, // 20251105 2025-11-01T07:32:59Z end=787_170_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 16, date_yyyymmdd: 20251106, start_chunk: 787_180_000 }, // 20251106 2025-11-01T07:32:59Z end=788_260_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 16, date_yyyymmdd: 20251107, start_chunk: 788_270_000 }, // 20251107 2025-11-01T07:32:59Z end=789_350_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 16, date_yyyymmdd: 20251108, start_chunk: 789_360_000 }, // 20251108 2025-11-01T07:32:59Z end=789_730_000 count=38
    ReplicaChunkIndexEntry { snapshot_idx: 17, date_yyyymmdd: 20251108, start_chunk: 789_733_000 }, // 20251108 2025-11-08T08:12:47Z end=789_733_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 17, date_yyyymmdd: 20251108, start_chunk: 789_740_000 }, // 20251108 2025-11-08T08:12:47Z end=790_430_000 count=70
    ReplicaChunkIndexEntry { snapshot_idx: 17, date_yyyymmdd: 20251109, start_chunk: 790_440_000 }, // 20251109 2025-11-08T08:12:47Z end=791_500_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 17, date_yyyymmdd: 20251110, start_chunk: 791_510_000 }, // 20251110 2025-11-08T08:12:47Z end=792_570_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 17, date_yyyymmdd: 20251111, start_chunk: 792_580_000 }, // 20251111 2025-11-08T08:12:47Z end=793_630_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 17, date_yyyymmdd: 20251112, start_chunk: 793_640_000 }, // 20251112 2025-11-08T08:12:47Z end=794_690_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 17, date_yyyymmdd: 20251113, start_chunk: 794_700_000 }, // 20251113 2025-11-08T08:12:47Z end=795_760_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 17, date_yyyymmdd: 20251114, start_chunk: 795_770_000 }, // 20251114 2025-11-08T08:12:47Z end=796_820_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 17, date_yyyymmdd: 20251115, start_chunk: 796_830_000 }, // 20251115 2025-11-08T08:12:47Z end=797_180_000 count=36
    ReplicaChunkIndexEntry { snapshot_idx: 18, date_yyyymmdd: 20251115, start_chunk: 797_181_000 }, // 20251115 2025-11-15T08:06:02Z end=797_181_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 18, date_yyyymmdd: 20251115, start_chunk: 797_190_000 }, // 20251115 2025-11-15T08:06:02Z end=797_880_000 count=70
    ReplicaChunkIndexEntry { snapshot_idx: 18, date_yyyymmdd: 20251116, start_chunk: 797_890_000 }, // 20251116 2025-11-15T08:06:02Z end=798_940_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 18, date_yyyymmdd: 20251117, start_chunk: 798_950_000 }, // 20251117 2025-11-15T08:06:02Z end=800_000_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 18, date_yyyymmdd: 20251118, start_chunk: 800_010_000 }, // 20251118 2025-11-15T08:06:02Z end=801_070_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 18, date_yyyymmdd: 20251119, start_chunk: 801_080_000 }, // 20251119 2025-11-15T08:06:02Z end=802_120_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 18, date_yyyymmdd: 20251120, start_chunk: 802_130_000 }, // 20251120 2025-11-15T08:06:02Z end=803_180_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 18, date_yyyymmdd: 20251121, start_chunk: 803_190_000 }, // 20251121 2025-11-15T08:06:02Z end=804_260_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 18, date_yyyymmdd: 20251122, start_chunk: 804_270_000 }, // 20251122 2025-11-15T08:06:02Z end=804_670_000 count=41
    ReplicaChunkIndexEntry { snapshot_idx: 19, date_yyyymmdd: 20251122, start_chunk: 804_679_000 }, // 20251122 2025-11-22T09:21:51Z end=804_679_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 19, date_yyyymmdd: 20251122, start_chunk: 804_680_000 }, // 20251122 2025-11-22T09:21:51Z end=805_330_000 count=66
    ReplicaChunkIndexEntry { snapshot_idx: 19, date_yyyymmdd: 20251123, start_chunk: 805_340_000 }, // 20251123 2025-11-22T09:21:51Z end=806_390_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 19, date_yyyymmdd: 20251124, start_chunk: 806_400_000 }, // 20251124 2025-11-22T09:21:51Z end=807_460_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 19, date_yyyymmdd: 20251125, start_chunk: 807_470_000 }, // 20251125 2025-11-22T09:21:51Z end=808_530_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 19, date_yyyymmdd: 20251126, start_chunk: 808_540_000 }, // 20251126 2025-11-22T09:21:51Z end=809_600_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 19, date_yyyymmdd: 20251127, start_chunk: 809_610_000 }, // 20251127 2025-11-22T09:21:51Z end=810_660_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 19, date_yyyymmdd: 20251128, start_chunk: 810_670_000 }, // 20251128 2025-11-22T09:21:51Z end=811_730_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 19, date_yyyymmdd: 20251129, start_chunk: 811_740_000 }, // 20251129 2025-11-22T09:21:51Z end=812_120_000 count=39
    ReplicaChunkIndexEntry { snapshot_idx: 20, date_yyyymmdd: 20251129, start_chunk: 812_129_000 }, // 20251129 2025-11-29T08:48:22Z end=812_129_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 20, date_yyyymmdd: 20251129, start_chunk: 812_130_000 }, // 20251129 2025-11-29T08:48:22Z end=812_800_000 count=68
    ReplicaChunkIndexEntry { snapshot_idx: 20, date_yyyymmdd: 20251130, start_chunk: 812_810_000 }, // 20251130 2025-11-29T08:48:22Z end=813_860_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 20, date_yyyymmdd: 20251201, start_chunk: 813_870_000 }, // 20251201 2025-11-29T08:48:22Z end=814_940_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 20, date_yyyymmdd: 20251202, start_chunk: 814_950_000 }, // 20251202 2025-11-29T08:48:22Z end=816_010_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 20, date_yyyymmdd: 20251203, start_chunk: 816_020_000 }, // 20251203 2025-11-29T08:48:22Z end=817_070_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 20, date_yyyymmdd: 20251204, start_chunk: 817_080_000 }, // 20251204 2025-11-29T08:48:22Z end=818_130_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 20, date_yyyymmdd: 20251205, start_chunk: 818_140_000 }, // 20251205 2025-11-29T08:48:22Z end=819_180_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 20, date_yyyymmdd: 20251206, start_chunk: 819_190_000 }, // 20251206 2025-11-29T08:48:22Z end=819_540_000 count=36
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251206, start_chunk: 819_541_000 }, // 20251206 2025-12-06T08:07:53Z end=819_541_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251206, start_chunk: 819_550_000 }, // 20251206 2025-12-06T08:07:53Z end=820_230_000 count=69
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251207, start_chunk: 820_240_000 }, // 20251207 2025-12-06T08:07:53Z end=821_300_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251208, start_chunk: 821_310_000 }, // 20251208 2025-12-06T08:07:53Z end=822_370_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251209, start_chunk: 822_380_000 }, // 20251209 2025-12-06T08:07:53Z end=823_440_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251210, start_chunk: 823_450_000 }, // 20251210 2025-12-06T08:07:53Z end=824_510_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251211, start_chunk: 824_520_000 }, // 20251211 2025-12-06T08:07:53Z end=825_580_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251212, start_chunk: 825_590_000 }, // 20251212 2025-12-06T08:07:53Z end=826_650_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251213, start_chunk: 826_660_000 }, // 20251213 2025-12-06T08:07:53Z end=827_730_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251214, start_chunk: 827_740_000 }, // 20251214 2025-12-06T08:07:53Z end=828_810_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251215, start_chunk: 828_820_000 }, // 20251215 2025-12-06T08:07:53Z end=829_880_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251216, start_chunk: 829_890_000 }, // 20251216 2025-12-06T08:07:53Z end=830_940_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251217, start_chunk: 830_950_000 }, // 20251217 2025-12-06T08:07:53Z end=832_020_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251218, start_chunk: 832_030_000 }, // 20251218 2025-12-06T08:07:53Z end=833_090_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251219, start_chunk: 833_100_000 }, // 20251219 2025-12-06T08:07:53Z end=834_160_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 21, date_yyyymmdd: 20251220, start_chunk: 834_170_000 }, // 20251220 2025-12-06T08:07:53Z end=834_510_000 count=35
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20251220, start_chunk: 834_515_000 }, // 20251220 2025-12-20T07:47:44Z end=834_515_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20251220, start_chunk: 834_520_000 }, // 20251220 2025-12-20T07:47:44Z end=835_240_000 count=73
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20251221, start_chunk: 835_250_000 }, // 20251221 2025-12-20T07:47:44Z end=836_300_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20251222, start_chunk: 836_310_000 }, // 20251222 2025-12-20T07:47:44Z end=837_360_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20251223, start_chunk: 837_370_000 }, // 20251223 2025-12-20T07:47:44Z end=838_420_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20251224, start_chunk: 838_430_000 }, // 20251224 2025-12-20T07:47:44Z end=839_480_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20251225, start_chunk: 839_490_000 }, // 20251225 2025-12-20T07:47:44Z end=840_540_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20251226, start_chunk: 840_550_000 }, // 20251226 2025-12-20T07:47:44Z end=841_600_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20251227, start_chunk: 841_610_000 }, // 20251227 2025-12-20T07:47:44Z end=842_660_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20251228, start_chunk: 842_670_000 }, // 20251228 2025-12-20T07:47:44Z end=843_720_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20251229, start_chunk: 843_730_000 }, // 20251229 2025-12-20T07:47:44Z end=844_780_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20251230, start_chunk: 844_790_000 }, // 20251230 2025-12-20T07:47:44Z end=845_840_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20251231, start_chunk: 845_850_000 }, // 20251231 2025-12-20T07:47:44Z end=846_900_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20260101, start_chunk: 846_910_000 }, // 20260101 2025-12-20T07:47:44Z end=847_960_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20260102, start_chunk: 847_970_000 }, // 20260102 2025-12-20T07:47:44Z end=849_020_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 22, date_yyyymmdd: 20260103, start_chunk: 849_030_000 }, // 20260103 2025-12-20T07:47:44Z end=849_330_000 count=31
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260103, start_chunk: 849_331_000 }, // 20260103 2026-01-03T07:05:33Z end=849_331_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260103, start_chunk: 849_340_000 }, // 20260103 2026-01-03T07:05:33Z end=850_070_000 count=74
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260104, start_chunk: 850_080_000 }, // 20260104 2026-01-03T07:05:33Z end=851_120_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260105, start_chunk: 851_130_000 }, // 20260105 2026-01-03T07:05:33Z end=852_180_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260106, start_chunk: 852_190_000 }, // 20260106 2026-01-03T07:05:33Z end=853_240_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260107, start_chunk: 853_250_000 }, // 20260107 2026-01-03T07:05:33Z end=854_300_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260108, start_chunk: 854_310_000 }, // 20260108 2026-01-03T07:05:33Z end=855_350_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260109, start_chunk: 855_360_000 }, // 20260109 2026-01-03T07:05:33Z end=856_400_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260110, start_chunk: 856_410_000 }, // 20260110 2026-01-03T07:05:33Z end=857_450_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260111, start_chunk: 857_460_000 }, // 20260111 2026-01-03T07:05:33Z end=858_490_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260112, start_chunk: 858_500_000 }, // 20260112 2026-01-03T07:05:33Z end=859_550_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260113, start_chunk: 859_560_000 }, // 20260113 2026-01-03T07:05:33Z end=860_600_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260114, start_chunk: 860_610_000 }, // 20260114 2026-01-03T07:05:33Z end=861_660_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260115, start_chunk: 861_670_000 }, // 20260115 2026-01-03T07:05:33Z end=862_710_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260116, start_chunk: 862_720_000 }, // 20260116 2026-01-03T07:05:33Z end=863_770_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 23, date_yyyymmdd: 20260117, start_chunk: 863_780_000 }, // 20260117 2026-01-03T07:05:33Z end=864_090_000 count=32
    ReplicaChunkIndexEntry { snapshot_idx: 24, date_yyyymmdd: 20260117, start_chunk: 864_091_000 }, // 20260117 2026-01-17T07:05:45Z end=864_091_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 24, date_yyyymmdd: 20260117, start_chunk: 864_100_000 }, // 20260117 2026-01-17T07:05:45Z end=864_830_000 count=74
    ReplicaChunkIndexEntry { snapshot_idx: 24, date_yyyymmdd: 20260118, start_chunk: 864_840_000 }, // 20260118 2026-01-17T07:05:45Z end=865_880_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 24, date_yyyymmdd: 20260119, start_chunk: 865_890_000 }, // 20260119 2026-01-17T07:05:45Z end=866_940_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 24, date_yyyymmdd: 20260120, start_chunk: 866_950_000 }, // 20260120 2026-01-17T07:05:45Z end=867_990_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 24, date_yyyymmdd: 20260121, start_chunk: 868_000_000 }, // 20260121 2026-01-17T07:05:45Z end=869_050_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 24, date_yyyymmdd: 20260122, start_chunk: 869_060_000 }, // 20260122 2026-01-17T07:05:45Z end=870_090_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 24, date_yyyymmdd: 20260123, start_chunk: 870_100_000 }, // 20260123 2026-01-17T07:05:45Z end=871_150_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 24, date_yyyymmdd: 20260124, start_chunk: 871_160_000 }, // 20260124 2026-01-17T07:05:45Z end=871_510_000 count=36
    ReplicaChunkIndexEntry { snapshot_idx: 25, date_yyyymmdd: 20260124, start_chunk: 871_512_000 }, // 20260124 2026-01-24T08:02:26Z end=871_512_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 25, date_yyyymmdd: 20260124, start_chunk: 871_520_000 }, // 20260124 2026-01-24T08:02:26Z end=872_210_000 count=70
    ReplicaChunkIndexEntry { snapshot_idx: 25, date_yyyymmdd: 20260125, start_chunk: 872_220_000 }, // 20260125 2026-01-24T08:02:26Z end=873_270_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 25, date_yyyymmdd: 20260126, start_chunk: 873_280_000 }, // 20260126 2026-01-24T08:02:26Z end=874_330_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 25, date_yyyymmdd: 20260127, start_chunk: 874_340_000 }, // 20260127 2026-01-24T08:02:26Z end=875_420_000 count=109
    ReplicaChunkIndexEntry { snapshot_idx: 25, date_yyyymmdd: 20260128, start_chunk: 875_430_000 }, // 20260128 2026-01-24T08:02:26Z end=876_500_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 25, date_yyyymmdd: 20260129, start_chunk: 876_510_000 }, // 20260129 2026-01-24T08:02:26Z end=877_580_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 25, date_yyyymmdd: 20260130, start_chunk: 877_590_000 }, // 20260130 2026-01-24T08:02:26Z end=878_660_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 25, date_yyyymmdd: 20260131, start_chunk: 878_670_000 }, // 20260131 2026-01-24T08:02:26Z end=879_020_000 count=36
    ReplicaChunkIndexEntry { snapshot_idx: 26, date_yyyymmdd: 20260131, start_chunk: 879_022_000 }, // 20260131 2026-01-31T08:04:34Z end=879_022_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 26, date_yyyymmdd: 20260131, start_chunk: 879_030_000 }, // 20260131 2026-01-31T08:04:34Z end=879_730_000 count=71
    ReplicaChunkIndexEntry { snapshot_idx: 26, date_yyyymmdd: 20260201, start_chunk: 879_740_000 }, // 20260201 2026-01-31T08:04:34Z end=880_800_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 26, date_yyyymmdd: 20260202, start_chunk: 880_810_000 }, // 20260202 2026-01-31T08:04:34Z end=881_870_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 26, date_yyyymmdd: 20260203, start_chunk: 881_880_000 }, // 20260203 2026-01-31T08:04:34Z end=882_950_000 count=108
    ReplicaChunkIndexEntry { snapshot_idx: 26, date_yyyymmdd: 20260204, start_chunk: 882_960_000 }, // 20260204 2026-01-31T08:04:34Z end=884_020_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 26, date_yyyymmdd: 20260205, start_chunk: 884_030_000 }, // 20260205 2026-01-31T08:04:34Z end=885_080_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 26, date_yyyymmdd: 20260206, start_chunk: 885_090_000 }, // 20260206 2026-01-31T08:04:34Z end=886_150_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 26, date_yyyymmdd: 20260207, start_chunk: 886_160_000 }, // 20260207 2026-01-31T08:04:34Z end=886_460_000 count=31
    ReplicaChunkIndexEntry { snapshot_idx: 27, date_yyyymmdd: 20260207, start_chunk: 886_461_000 }, // 20260207 2026-02-07T07:04:22Z end=886_461_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 27, date_yyyymmdd: 20260207, start_chunk: 886_470_000 }, // 20260207 2026-02-07T07:04:22Z end=887_200_000 count=74
    ReplicaChunkIndexEntry { snapshot_idx: 27, date_yyyymmdd: 20260208, start_chunk: 887_210_000 }, // 20260208 2026-02-07T07:04:22Z end=888_250_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 27, date_yyyymmdd: 20260209, start_chunk: 888_260_000 }, // 20260209 2026-02-07T07:04:22Z end=889_300_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 27, date_yyyymmdd: 20260210, start_chunk: 889_310_000 }, // 20260210 2026-02-07T07:04:22Z end=890_350_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 27, date_yyyymmdd: 20260211, start_chunk: 890_360_000 }, // 20260211 2026-02-07T07:04:22Z end=891_400_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 27, date_yyyymmdd: 20260212, start_chunk: 891_410_000 }, // 20260212 2026-02-07T07:04:22Z end=892_450_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 27, date_yyyymmdd: 20260213, start_chunk: 892_460_000 }, // 20260213 2026-02-07T07:04:22Z end=893_500_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 27, date_yyyymmdd: 20260214, start_chunk: 893_510_000 }, // 20260214 2026-02-07T07:04:22Z end=894_540_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 27, date_yyyymmdd: 20260215, start_chunk: 894_550_000 }, // 20260215 2026-02-07T07:04:22Z end=895_590_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 27, date_yyyymmdd: 20260216, start_chunk: 895_600_000 }, // 20260216 2026-02-07T07:04:22Z end=896_130_000 count=54
    ReplicaChunkIndexEntry { snapshot_idx: 28, date_yyyymmdd: 20260216, start_chunk: 896_135_000 }, // 20260216 2026-02-16T12:21:56Z end=896_135_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 28, date_yyyymmdd: 20260216, start_chunk: 896_140_000 }, // 20260216 2026-02-16T12:21:56Z end=896_640_000 count=51
    ReplicaChunkIndexEntry { snapshot_idx: 28, date_yyyymmdd: 20260217, start_chunk: 896_650_000 }, // 20260217 2026-02-16T12:21:56Z end=897_690_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 28, date_yyyymmdd: 20260218, start_chunk: 897_700_000 }, // 20260218 2026-02-16T12:21:56Z end=898_740_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 28, date_yyyymmdd: 20260219, start_chunk: 898_750_000 }, // 20260219 2026-02-16T12:21:56Z end=899_790_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 28, date_yyyymmdd: 20260220, start_chunk: 899_800_000 }, // 20260220 2026-02-16T12:21:56Z end=900_840_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 28, date_yyyymmdd: 20260221, start_chunk: 900_850_000 }, // 20260221 2026-02-16T12:21:56Z end=901_170_000 count=33
    ReplicaChunkIndexEntry { snapshot_idx: 29, date_yyyymmdd: 20260221, start_chunk: 901_177_000 }, // 20260221 2026-02-21T07:34:29Z end=901_177_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 29, date_yyyymmdd: 20260221, start_chunk: 901_180_000 }, // 20260221 2026-02-21T07:34:29Z end=901_900_000 count=73
    ReplicaChunkIndexEntry { snapshot_idx: 29, date_yyyymmdd: 20260222, start_chunk: 901_910_000 }, // 20260222 2026-02-21T07:34:29Z end=902_950_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 29, date_yyyymmdd: 20260223, start_chunk: 902_960_000 }, // 20260223 2026-02-21T07:34:29Z end=904_020_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 29, date_yyyymmdd: 20260224, start_chunk: 904_030_000 }, // 20260224 2026-02-21T07:34:29Z end=905_070_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 29, date_yyyymmdd: 20260225, start_chunk: 905_080_000 }, // 20260225 2026-02-21T07:34:29Z end=906_130_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 29, date_yyyymmdd: 20260226, start_chunk: 906_140_000 }, // 20260226 2026-02-21T07:34:29Z end=907_190_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 29, date_yyyymmdd: 20260227, start_chunk: 907_200_000 }, // 20260227 2026-02-21T07:34:29Z end=908_240_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 29, date_yyyymmdd: 20260228, start_chunk: 908_250_000 }, // 20260228 2026-02-21T07:34:29Z end=909_300_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 29, date_yyyymmdd: 20260301, start_chunk: 909_310_000 }, // 20260301 2026-02-21T07:34:29Z end=909_610_000 count=31
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260301, start_chunk: 909_615_000 }, // 20260301 2026-03-01T07:06:51Z end=909_615_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260301, start_chunk: 909_620_000 }, // 20260301 2026-03-01T07:06:51Z end=910_360_000 count=75
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260302, start_chunk: 910_370_000 }, // 20260302 2026-03-01T07:06:51Z end=911_420_000 count=106
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260303, start_chunk: 911_430_000 }, // 20260303 2026-03-01T07:06:51Z end=912_490_000 count=107
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260304, start_chunk: 912_500_000 }, // 20260304 2026-03-01T07:06:51Z end=913_540_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260305, start_chunk: 913_550_000 }, // 20260305 2026-03-01T07:06:51Z end=914_580_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260306, start_chunk: 914_590_000 }, // 20260306 2026-03-01T07:06:51Z end=915_620_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260307, start_chunk: 915_630_000 }, // 20260307 2026-03-01T07:06:51Z end=916_670_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260308, start_chunk: 916_680_000 }, // 20260308 2026-03-01T07:06:51Z end=917_710_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260309, start_chunk: 917_720_000 }, // 20260309 2026-03-01T07:06:51Z end=918_750_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260310, start_chunk: 918_760_000 }, // 20260310 2026-03-01T07:06:51Z end=919_790_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260311, start_chunk: 919_800_000 }, // 20260311 2026-03-01T07:06:51Z end=920_830_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260312, start_chunk: 920_840_000 }, // 20260312 2026-03-01T07:06:51Z end=921_870_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260313, start_chunk: 921_880_000 }, // 20260313 2026-03-01T07:06:51Z end=922_910_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 30, date_yyyymmdd: 20260314, start_chunk: 922_920_000 }, // 20260314 2026-03-01T07:06:51Z end=923_210_000 count=30
    ReplicaChunkIndexEntry { snapshot_idx: 31, date_yyyymmdd: 20260314, start_chunk: 923_212_000 }, // 20260314 2026-03-14T07:05:59Z end=923_212_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 31, date_yyyymmdd: 20260314, start_chunk: 923_220_000 }, // 20260314 2026-03-14T07:05:59Z end=923_950_000 count=74
    ReplicaChunkIndexEntry { snapshot_idx: 31, date_yyyymmdd: 20260315, start_chunk: 923_960_000 }, // 20260315 2026-03-14T07:05:59Z end=924_980_000 count=103
    ReplicaChunkIndexEntry { snapshot_idx: 31, date_yyyymmdd: 20260316, start_chunk: 924_990_000 }, // 20260316 2026-03-14T07:05:59Z end=926_020_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 31, date_yyyymmdd: 20260317, start_chunk: 926_030_000 }, // 20260317 2026-03-14T07:05:59Z end=927_060_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 31, date_yyyymmdd: 20260318, start_chunk: 927_070_000 }, // 20260318 2026-03-14T07:05:59Z end=928_100_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 31, date_yyyymmdd: 20260319, start_chunk: 928_110_000 }, // 20260319 2026-03-14T07:05:59Z end=929_150_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 31, date_yyyymmdd: 20260320, start_chunk: 929_160_000 }, // 20260320 2026-03-14T07:05:59Z end=930_180_000 count=103
    ReplicaChunkIndexEntry { snapshot_idx: 31, date_yyyymmdd: 20260321, start_chunk: 930_190_000 }, // 20260321 2026-03-14T07:05:59Z end=930_590_000 count=41
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260321, start_chunk: 930_593_000 }, // 20260321 2026-03-21T09:33:29Z end=930_593_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260321, start_chunk: 930_600_000 }, // 20260321 2026-03-21T09:33:29Z end=931_220_000 count=63
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260322, start_chunk: 931_230_000 }, // 20260322 2026-03-21T09:33:29Z end=932_260_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260323, start_chunk: 932_270_000 }, // 20260323 2026-03-21T09:33:29Z end=933_290_000 count=103
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260324, start_chunk: 933_300_000 }, // 20260324 2026-03-21T09:33:29Z end=934_320_000 count=103
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260325, start_chunk: 934_330_000 }, // 20260325 2026-03-21T09:33:29Z end=935_360_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260326, start_chunk: 935_370_000 }, // 20260326 2026-03-21T09:33:29Z end=936_410_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260327, start_chunk: 936_420_000 }, // 20260327 2026-03-21T09:33:29Z end=937_460_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260328, start_chunk: 937_470_000 }, // 20260328 2026-03-21T09:33:29Z end=938_500_000 count=104
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260329, start_chunk: 938_510_000 }, // 20260329 2026-03-21T09:33:29Z end=939_550_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260330, start_chunk: 939_560_000 }, // 20260330 2026-03-21T09:33:29Z end=940_600_000 count=105
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260331, start_chunk: 940_610_000 }, // 20260331 2026-03-21T09:33:29Z end=941_770_000 count=117
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260401, start_chunk: 941_780_000 }, // 20260401 2026-03-21T09:33:29Z end=942_870_000 count=110
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260402, start_chunk: 942_880_000 }, // 20260402 2026-03-21T09:33:29Z end=944_030_000 count=116
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260403, start_chunk: 944_040_000 }, // 20260403 2026-03-21T09:33:29Z end=945_250_000 count=122
    ReplicaChunkIndexEntry { snapshot_idx: 32, date_yyyymmdd: 20260404, start_chunk: 945_260_000 }, // 20260404 2026-03-21T09:33:29Z end=945_640_000 count=39
    ReplicaChunkIndexEntry { snapshot_idx: 33, date_yyyymmdd: 20260404, start_chunk: 945_647_000 }, // 20260404 2026-04-04T07:35:15Z end=945_647_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 33, date_yyyymmdd: 20260404, start_chunk: 945_650_000 }, // 20260404 2026-04-04T07:35:15Z end=946_480_000 count=84
    ReplicaChunkIndexEntry { snapshot_idx: 33, date_yyyymmdd: 20260405, start_chunk: 946_490_000 }, // 20260405 2026-04-04T07:35:15Z end=947_720_000 count=124
    ReplicaChunkIndexEntry { snapshot_idx: 33, date_yyyymmdd: 20260406, start_chunk: 947_730_000 }, // 20260406 2026-04-04T07:35:15Z end=948_950_000 count=123
    ReplicaChunkIndexEntry { snapshot_idx: 33, date_yyyymmdd: 20260407, start_chunk: 948_960_000 }, // 20260407 2026-04-04T07:35:15Z end=950_180_000 count=123
    ReplicaChunkIndexEntry { snapshot_idx: 33, date_yyyymmdd: 20260408, start_chunk: 950_190_000 }, // 20260408 2026-04-04T07:35:15Z end=951_420_000 count=124
    ReplicaChunkIndexEntry { snapshot_idx: 33, date_yyyymmdd: 20260409, start_chunk: 951_430_000 }, // 20260409 2026-04-04T07:35:15Z end=952_690_000 count=127
    ReplicaChunkIndexEntry { snapshot_idx: 33, date_yyyymmdd: 20260410, start_chunk: 952_700_000 }, // 20260410 2026-04-04T07:35:15Z end=953_960_000 count=127
    ReplicaChunkIndexEntry { snapshot_idx: 33, date_yyyymmdd: 20260411, start_chunk: 953_970_000 }, // 20260411 2026-04-04T07:35:15Z end=955_230_000 count=127
    ReplicaChunkIndexEntry { snapshot_idx: 33, date_yyyymmdd: 20260412, start_chunk: 955_240_000 }, // 20260412 2026-04-04T07:35:15Z end=956_510_000 count=128
    ReplicaChunkIndexEntry { snapshot_idx: 33, date_yyyymmdd: 20260413, start_chunk: 956_520_000 }, // 20260413 2026-04-04T07:35:15Z end=956_970_000 count=46
    ReplicaChunkIndexEntry { snapshot_idx: 34, date_yyyymmdd: 20260413, start_chunk: 956_971_000 }, // 20260413 2026-04-13T08:41:11Z end=956_971_000 count=1
    ReplicaChunkIndexEntry { snapshot_idx: 34, date_yyyymmdd: 20260413, start_chunk: 956_980_000 }, // 20260413 2026-04-13T08:41:11Z end=957_770_000 count=80
    ReplicaChunkIndexEntry { snapshot_idx: 34, date_yyyymmdd: 20260414, start_chunk: 957_780_000 }, // 20260414 2026-04-13T08:41:11Z end=959_020_000 count=125
    ReplicaChunkIndexEntry { snapshot_idx: 34, date_yyyymmdd: 20260415, start_chunk: 959_030_000 }, // 20260415 2026-04-13T08:41:11Z end=960_280_000 count=126
];
