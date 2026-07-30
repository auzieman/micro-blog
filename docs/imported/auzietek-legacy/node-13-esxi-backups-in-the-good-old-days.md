---
title: "ESXi Backups in the good old days,"
slug: "esxi-backups-in-the-good-old-days"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/13"
source_id: "node-13"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  []
---

This may need various updates,

It was a good while ago, however it is likely a supported route even today. Not this does result in pauses to guests however its very brief, use of advanced storage like NetApp can make this a near real time activity.

```
#!/bin/bash
cd /etc/snapshots/
for vmhost in `cat vmhosts`
do
 echo "Working with VMHost:$vmhost"
 for vmguest in `cat $vmhost.vmguests`
 do
  echo "Working with VMGuest:$vmguest, getting vmgid"
  vmgid=`ssh $vmhost "vim-cmd /vmsvc/getallvms|grep $vmguest|cut -f1 -d' '"`
  echo "Working with VMGuest:$vmguest, got vmgid:$vmgid, getting vmgstate"
  vmgstate=`ssh $vmhost "vim-cmd /vmsvc/power.getstate $vmgid|grep -v Retrieved"`
  vmgstate=`echo $vmgstate|sed "s/ /_/g"`
  echo "VMGuest:$vmguest was VMGuest ID:$vmgid and is in the state:$vmgstate"
  if [ $vmgstate == "Powered_on" ]
   then
      echo "Issuing vmguest suspend and waiting 30 seconds"
      ssh $vmhost "vim-cmd /vmsvc/power.suspend $vmgid"
      sleep 30
      vmgstate=`ssh $vmhost "vim-cmd /vmsvc/power.getstate $vmgid|grep -v Retrieved"`
                    vmgstate=`echo $vmgstate|sed "s/ /_/g"`
                    echo "VMGuest:$vmguest was VMGuest ID:$vmgid and is in the state:$vmgstate"

                    while [ $vmgstate != "Suspended" ]
       do
   echo Wait more
   sleep 30
   vmgstate=`ssh $vmhost "vim-cmd /vmsvc/power.getstate $vmgid|grep -v Retrieved"`
                 vmgstate=`echo $vmgstate|sed "s/ /_/g"`
      done

                    if [ $vmgstate == "Suspended" ]
      then
        echo "System looks suspended Taking snapshot"
        ./esx-snap.sh $vmguest
      fi

      echo "Resuming VMGuest"
      ssh $vmhost "vim-cmd /vmsvc/power.on $vmgid"
                    while [ $vmgstate == "Suspended" ]
       do
   echo Wait more
   sleep 30
   vmgstate=`ssh $vmhost "vim-cmd /vmsvc/power.getstate $vmgid|grep -v Retrieved"`
                 vmgstate=`echo $vmgstate|sed "s/ /_/g"`
      done

  fi
 done
done
exit
```

Where esx-snap.sh looks like this,,

```
#!/bin/bash
vol=$1
if [ ! -f /etc/snapshots/$vol.last ]
then
echo "0" > /etc/snapshots/$vol.last
fi
last=`cat /etc/snapshots/$vol.last`
if [ $last -gt '0' ]
then
 last=0
fi

if [ ! -d /mnt/snapshots/$vol.$last ]
then
  mkdir /mnt/snapshots/$vol.$last
fi
umount /mnt/snapshots/$vol.$last

 lvremove -f /dev/nas0/$vol.$last
sizeraw=`du -sk /mnt/$vol|cut -f1`
echo $sizeraw
size=`echo $sizeraw"*1.15"|bc`
echo $size
 lvcreate -L`echo $size"k"` -s -n $vol.$last /dev/nas0/$vol
 mount /dev/nas0/$vol.$last /mnt/snapshots/$vol.$last
let last=last+1
echo $last > /etc/snapshots/$vol.last
exit
```

Blog tags

[linux](/taxonomy/term/7)

[VMWare](/taxonomy/term/8)

Submitted by auzieman
 on Wed, 07/20/2022 - 09:21

