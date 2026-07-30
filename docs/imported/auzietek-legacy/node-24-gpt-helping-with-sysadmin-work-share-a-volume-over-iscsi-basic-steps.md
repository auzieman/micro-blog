---
title: "GPT Helping with Sysadmin work, share a volume over iscsi basic steps"
slug: "gpt-helping-with-sysadmin-work-share-a-volume-over-iscsi-basic-steps"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/24"
source_id: "node-24"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  []
---

Me,

This may be a bit tricky but if you can, assuming a CentOS7 host with 500TB maybe raid 11 storage, could you describe using shell, creating a volume say 1TB in size then sharing it over iscsi, then on a client centos7 mounting that shared volume as perhaps /data1

GPT:

Sure, I can guide you through the steps to create a 1TB volume on a CentOS7 host with 500TB RAID 11 storage, share it over iSCSI, and then mount it on a client CentOS7 machine as /data1. Here are the steps:

First, you need to create a logical volume on the CentOS7 host using the lvcreate command. Assuming you want to create a 1TB volume, you can run the following command:

```
sudo lvcreate --size 1T --name myvol /dev/<your_volume_group>
```

Replace <your_volume_group> with the name of your volume group, which should be something like vg0. This will create a logical volume named myvol that is 1TB in size.

Next, you need to format the logical volume with a file system. You can use the mkfs command to do this:

```
sudo mkfs.ext4 /dev/<your_volume_group>/myvol
```

This will format the logical volume with the ext4 file system.

Now you need to create an iSCSI target on the CentOS7 host using the tgtadm command. You can run the following command:

```
sudo tgtadm --lld iscsi --op new --mode target --tid 1 -T iqn.2023-02.com.example:myvol
```

This will create an iSCSI target with the name iqn.2023-02.com.example:myvol.

Next, you need to create a logical unit (LUN) for the iSCSI target using the tgtadm command. You can run the following command:

```
sudo tgtadm --lld iscsi --op new --mode logicalunit --tid 1 --lun 1 -b /dev/<your_volume_group>/myvol
```

This will create a LUN for the iSCSI target that is backed by the logical volume you created earlier.

Now you need to start the iSCSI target using the tgt-admin command:

```
sudo systemctl start tgtd
```

This will start the iSCSI target service.

On the client CentOS7 machine, you need to install the iscsi-initiator-utils package:

```
sudo yum install iscsi-initiator-utils
```

This will install the necessary utilities to connect to an iSCSI target.

Next, you need to discover the iSCSI target from the client machine using the iscsiadm command:

```
sudo iscsiadm -m discovery -t st -p <ip_address_of_centos7_host>
```

Replace <ip_address_of_centos7_host> with the IP address of the CentOS7 host.

Once the iSCSI target is discovered, you need to log in to it using the iscsiadm command:

```
sudo iscsiadm -m node -T iqn.2023-02.com.example:myvol -p <ip_address_of_centos7_host> -l
```

Replace <ip_address_of_centos7_host> with the IP address of the CentOS7 host.

Finally, you need to mount the iSCSI volume on the client machine using the mount command:

```
sudo mount /dev/<iscsi_device> /data1
```

Replace <iscsi_device> with the iSCSI device name, which should be something like /dev/sdb.

That's it! You should now have a 1TB volume mounted on the client machine at /data1 that is backed by the logical volume on the CentOS7 host and shared over

Blog tags

[linux](/taxonomy/term/7)

[iscsi](/taxonomy/term/39)

[ChatGPT](/taxonomy/term/23)

Submitted by auzieman
 on Tue, 02/28/2023 - 15:31

