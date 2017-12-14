diff --git a/.gitignore b/.gitignore
index f36319d..7837c4b 100644
--- a/.gitignore
+++ b/.gitignore
@@ -5,9 +5,9 @@ Thumbs.db
 *_bkp*
 **/*_bkp*
 api/Mathematica
-api/**/*.egg-info
-api/**/filessage.txt
-api/**/filespy.txt
-api/**/dist
-api/**/build
-api/**/*.pyc
+api/**/**/*.egg-info
+api/**/**/filessage.txt
+api/**/**/filespy.txt
+api/**/**/dist
+api/**/**/build
+api/**/**/*.pyc
diff --git a/README.md b/README.md
index 77c5ffb..b653f31 100644
--- a/README.md
+++ b/README.md
@@ -122,13 +122,19 @@ Make sure to replace everything in `<...>` with your job information.
 
 4) If your module depends on previous modules, add these dependencies to the file `${CRUNCH_ROOT}/modules/dependencies/<module>`, where `<module>` is the name of your module.
 
-5) Create your working directory `<dir>` and navigate to it
+5) Create your working directory `<work_dir>` and navigate to it
 
 ```
-   cd <dir>
+   cd <work_dir>
 ```
 
-6) Copy over your template
+6) Add an empty file `.DBCrunch` to the working directory to designate it as such
+
+```
+   touch .DBCrunch
+```
+
+7) Copy over your template
 
 ```
    crunch template <module> <controller>
@@ -136,19 +142,19 @@ Make sure to replace everything in `<...>` with your job information.
 
 where `<controller>` is a name you choose for the segment of your database that you wish to process.
 
-7) Navigate to the controller directory
+8) Navigate to the controller directory
 
 ```
    cd <module>/<controller>
 ```
 
-8) Begin processing
+9) Begin processing
 
 ```
-   crunch
+   crunch submit
 ```
 
-9) Monitor your progress
+10) Monitor your progress
 
 ```
    crunch monitor <s>
@@ -156,19 +162,19 @@ where `<controller>` is a name you choose for the segment of your database that
 
 where `<s>` is the refresh interval. Use `Ctrl-C` to exit.
 
-10) If you need to terminate the process for any reason, use:
+11) If you need to terminate the process for any reason, use:
 
 ```
    crunch cancel <module> <controller>
 ```
 
-11) In order to requeue jobs that may have failed, use:
+12) In order to requeue jobs that may have failed, use:
 
 ```
    crunch requeue <module> <controller>
 ```
 
-12) If you wish to reset the entire controller directory to initial conditions, use:
+13) If you wish to reset the entire controller directory to initial conditions, use:
 
 ```
    crunch reset <module> <controller>
diff --git a/api/db/mongojoin b/api/db/mongojoin
--- a/api/db/mongojoin
+++ b/api/db/mongojoin
@@ -1 +1 @@
-Subproject commit b15eb920d430bebdf247a45077481b9a77ae7326
+Subproject commit b15eb920d430bebdf247a45077481b9a77ae7326-dirty
diff --git a/api/workload/crunch_slurm/crunch_slurm/__init__.py b/api/workload/crunch_slurm/crunch_slurm/__init__.py
index 8321438..afd531c 100644
--- a/api/workload/crunch_slurm/crunch_slurm/__init__.py
+++ b/api/workload/crunch_slurm/crunch_slurm/__init__.py
@@ -1,3 +1,3 @@
 #!/shared/apps/python/Python-2.7.5/INSTALL/bin/python
 
-from slurm import *;
\ No newline at end of file
+from slurm import *
\ No newline at end of file
diff --git a/api/workload/crunch_slurm/crunch_slurm/slurm.py b/api/workload/crunch_slurm/crunch_slurm/slurm.py
index a9bf751..563b81d 100644
--- a/api/workload/crunch_slurm/crunch_slurm/slurm.py
+++ b/api/workload/crunch_slurm/crunch_slurm/slurm.py
@@ -1,71 +1,71 @@
-from signal import signal,SIGPIPE,SIG_DFL;
-from subprocess import Popen,PIPE;
+from signal import signal, SIGPIPE, SIG_DFL
+from subprocess import Popen, PIPE
 
 def default_sigpipe():
-    signal(SIGPIPE,SIG_DFL);
+    signal(SIGPIPE, SIG_DFL)
 
 def get_maxtimelimit(partition):
-    return Popen("sinfo -h -o '%l %P' | grep -E '"+partition+"\*?\s*$' | sed 's/\s\s*/ /g' | sed 's/*//g' | cut -d' ' -f1 | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0];
+    return Popen("sinfo -h -o '%l %P' | grep -E '" + partition + "\*?\s*$' | sed 's/\s\s*/ /g' | sed 's/*//g' | cut -d' ' -f1 | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0]
 
 def get_pendjobnamespaths(username):
-    return [x.split() for x in Popen("squeue -h -u "+username+" -o '%T %j %.130Z' | grep 'crunch_' | grep 'PENDING' | cut -d' ' -f2,3 | grep -v '_controller' | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(",")];
+    return [x.split() for x in Popen("squeue -h -u " + username + " -o '%T %j %.130Z' | grep 'crunch_' | grep 'PENDING' | cut -d' ' -f2,3 | grep -v '_controller' | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(",")]
 
 def get_nglobaljobs(username):
-    return eval(Popen("squeue -h -r -u "+username+" -o '%.130j %.2t' | grep 'crunch_' | grep -v ' CG ' | wc -l | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);
+    return eval(Popen("squeue -h -r -u " + username + " -o '%.130j %.2t' | grep 'crunch_' | grep -v ' CG ' | wc -l | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
 
 def get_nlocaljobs(username,modname,controllername):
-    return eval(Popen("squeue -h -r -u "+username+" -o '%.130j %.2t' | grep 'crunch_' | grep -v ' CG ' | grep 'crunch_"+modname+"_"+controllername+"_job_' | wc -l | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);
+    return eval(Popen("squeue -h -r -u " + username + " -o '%.130j %.2t' | grep 'crunch_' | grep -v ' CG ' | grep 'crunch_" + modname + "_" + controllername + "_job_' | wc -l | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
 
 def controllerjobsrunningq(username,modname,controllername):
-    return eval(Popen("squeue -h -u "+username+" -o '%j' | grep 'crunch_"+modname+"_"+controllername+"_' | wc -l | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0])>0;
+    return eval(Popen("squeue -h -u " + username + " -o '%j' | grep 'crunch_" + modname + "_" + controllername + "_' | wc -l | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])>0
 
 def prevcontrollerjobsrunningq(username,dependencies):
-    if len(dependencies)==0:
-        njobsrunning=0;
+    if len(dependencies) == 0:
+        njobsrunning = 0
     else:
-        grepstr="\|".join(dependencies);
-        njobsrunning=eval(Popen("squeue -h -u "+username+" -o '%j' | grep 'crunch_\("+grepstr+"\)_' | wc -l | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);
-    return njobsrunning>0;
+        grepstr = "\|".join(dependencies)
+        njobsrunning = eval(Popen("squeue -h -u " + username + " -o '%j' | grep 'crunch_\(" + grepstr + "\)_' | wc -l | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
+    return njobsrunning > 0
 
 def get_submitjob(jobpath,jobname):
-    return Popen("sbatch "+jobpath+"/"+jobname+".job",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].rstrip("\n").split()[-1];
+    return Popen("sbatch " + jobpath + "/" + jobname + ".job", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].rstrip("\n").split()[-1]
 
 def releaseheldjobs(username,modname,controllername):
-    Popen("for job in $(squeue -h -u "+username+" -o '%j %A %r' | grep 'crunch_"+modname+"_"+controllername+"_' | grep 'job requeued in held state' | sed 's/\s\s*/ /g' | cut -d' ' -f2); do scontrol release $job; done",shell=True,preexec_fn=default_sigpipe);
+    Popen("for job in $(squeue -h -u " + username + " -o '%j %A %r' | grep 'crunch_" + modname + "_" + controllername + "_' | grep 'job requeued in held state' | sed 's/\s\s*/ /g' | cut -d' ' -f2); do scontrol release $job; done", shell=True,preexec_fn=default_sigpipe)
 
 def get_partitionsidle(partitions):
-    greppartitions="|".join(partitions);
-    return Popen("sinfo -h -o '%t %c %D %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'idle' | awk '$0=$1\" \"$2*$3\" \"$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
+    greppartitions = "|".join(partitions)
+    return Popen("sinfo -h -o '%t %c %D %P' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'idle' | awk '$0=$1\" \"$2*$3\" \"$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(',')
 
 def get_partitionscomp(partitions):
-    greppartitions="|".join(partitions);
-    return Popen("sinfo -h -o '%t %c %D %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'comp' | awk '$0=$1\" \"$2*$3\" \"$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
+    greppartitions = "|".join(partitions)
+    return Popen("sinfo -h -o '%t %c %D %P' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'comp' | awk '$0=$1\" \"$2*$3\" \"$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(',')
 
 def get_partitionsrun(partitions):
-    greppartitions="|".join(partitions);
-    return Popen("squeue -h -o '%L %T %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'RUNNING' | sed 's/^\([0-9][0-9]:[0-9][0-9]\s\)/00:\1/g' | sed 's/^\([0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0\1/g' | sed 's/^\([0-9][0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0-\1/g' | sort -k1,1 | cut -d' ' -f3 | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
+    greppartitions = "|".join(partitions)
+    return Popen("squeue -h -o '%L %T %P' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'RUNNING' | sed 's/^\([0-9][0-9]:[0-9][0-9]\s\)/00:\1/g' | sed 's/^\([0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0\1/g' | sed 's/^\([0-9][0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0-\1/g' | sort -k1,1 | cut -d' ' -f3 | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(',')
 
 def get_partitionspend(partitions):
-    greppartitions="|".join(partitions);
-    return Popen("sinfo -h -o '%t %c %P' --partition=$(squeue -h -o '%T %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'PENDING' | sort -k2,2 -u | cut -d' ' -f2 | tr '\n' ',' | head -c -1) | grep 'alloc' | sort -k2,2n | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
+    greppartitions = "|".join(partitions)
+    return Popen("sinfo -h -o '%t %c %P' --partition=$(squeue -h -o '%T %P' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'PENDING' | sort -k2,2 -u | cut -d' ' -f2 | tr '\n' ',' | head -c -1) | grep 'alloc' | sort -k2,2n | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(',')
 
 def get_partitionncorespernode(partition):
-    return eval(Popen("sinfo -h -p '"+partition+"' -o '%c' | head -n1 | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);
+    return eval(Popen("sinfo -h -p '" + partition + "' -o '%c' | head -n1 | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
 
 def get_maxnnodes(partition):
-    return eval(Popen("scontrol show partition '"+partition+"' | grep 'MaxNodes=' | sed 's/^.*\sMaxNodes=\([0-9]*\)\s.*$/\\1/g' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].rstrip("\n"));
+    return eval(Popen("scontrol show partition '" + partition + "' | grep 'MaxNodes=' | sed 's/^.*\sMaxNodes=\([0-9]*\)\s.*$/\\1/g' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].rstrip("\n"))
 
 def get_maxjobcount():
-    return eval(Popen("scontrol show config | grep 'MaxJobCount' | sed 's/\s//g' | cut -d'=' -f2 | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);
+    return eval(Popen("scontrol show config | grep 'MaxJobCount' | sed 's/\s//g' | cut -d'=' -f2 | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
 
 def get_maxstepcount():
-    return eval(Popen("scontrol show config | grep 'MaxStepCount' | sed 's/\s//g' | cut -d'=' -f2 | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);
+    return eval(Popen("scontrol show config | grep 'MaxStepCount' | sed 's/\s//g' | cut -d'=' -f2 | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
 
 def get_controllerstats(controllerjobid):
-    return Popen("sacct -n -j "+controllerjobid+" -o 'Partition%30,Timelimit,NNodes,NCPUs' | head -n1 | sed 's/^\s*//g' | sed 's/\s\s*/ /g' | tr ' ' ',' | tr '\n' ',' | head -c -2",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(",");
+    return Popen("sacct -n -j " + controllerjobid + " -o 'Partition%30,Timelimit,NNodes,NCPUs' | head -n1 | sed 's/^\s*//g' | sed 's/\s\s*/ /g' | tr ' ' ',' | tr '\n' ',' | head -c -2", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(",")
 
 def get_controllerpath(controllerjobid):
-    return Popen("squeue -h -j "+controllerjobid+" -o '%Z' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0];
+    return Popen("squeue -h -j " + controllerjobid + " -o '%Z' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0]
 
 def get_exitcode(jobid):
-    return Popen("sacct -n -j "+jobid+" -o 'ExitCode' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | head -c -2",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0];
\ No newline at end of file
+    return Popen("sacct -n -j " + jobid + " -o 'ExitCode' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | head -c -2", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0]
\ No newline at end of file
diff --git a/bin/bash_completion b/bin/bash_completion
index c8182d1..a53108f 100644
--- a/bin/bash_completion
+++ b/bin/bash_completion
@@ -4,9 +4,10 @@ _crunch()
     local cur
     cur=${COMP_WORDS[COMP_CWORD]}
     #prev=${COMP_WORDS[COMP_CWORD-1]}
+
     case ${COMP_CWORD} in
         1)
-            COMPREPLY=($(compgen -W "--modules_dir cancel monitor requeue reset submit template" -- ${cur}))
+            COMPREPLY=($(compgen -W "--modules-dir cancel monitor requeue reset submit template test" -- ${cur}))
             ;;
         #2)
         #    case ${prev} in
@@ -18,84 +19,87 @@ _crunch()
         #            ;;
         #    esac
         #    ;;
-        2)
-            if [[ "${COMP_WORDS[COMP_CWORD-1]}" != "--modules_dir" ]]
-            then
-                modules_dir=$(pwd -P)
+        *)
+            case ${COMP_WORDS[1]} in
+                --modules_dir)
+                    modules_dir=$(eval echo ${COMP_WORDS[COMP_CWORD-2]})
+                    right_shift=2
+                    ;;
+                *)
+                    modules_dir=$(pwd -P)
+                    right_shift=0
+                    ;;
+            esac
 
-                startpath=$(cd ${modules_dir} && pwd -P)
-                branches=$(find ${startpath} -path '*/.*' -prune -o -type d -exec bash -c 'echo "$(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g" | grep -o / | wc -l) $(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g")"' \; | grep -v '/jobs/' | sort -u | tr '\n' ',')
-                maxdepth=$(echo ${branches} | tr ',' '\n' | cut -d' ' -f1 | sort -u | tail -n1)
-                leaves=$(echo ${branches} | tr ',' '\n' | grep "^${maxdepth} " | cut -d' ' -f2 | tr '\n' ',')
-                moddirpath=$(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1,2 --complement | rev | head -n1)
-                #modnames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f2 | rev))
-                #controllernames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1 | rev))
+            startpath=$(cd ${modules_dir} && pwd -P)
 
-                modcontrollernames=$(find ${moddirpath} -mindepth 1 -path '*/.*' -prune -o -type d -regextype posix-extended -regex ".*/${cur}.*" -print 2>/dev/null | grep -v '/jobs' | rev | cut -d'/' -f1 | rev)
-                
-                COMPREPLY=($(compgen -W "${modcontrollernames}" -- ${cur}))
-            fi
-            ;;
-        3)
-            if [[ "${COMP_WORDS[COMP_CWORD-2]}" != "--modules_dir" ]]
-            then
-                modules_dir=$(pwd -P)
-
-                startpath=$(cd ${modules_dir} && pwd -P)
-                branches=$(find ${startpath} -path '*/.*' -prune -o -type d -exec bash -c 'echo "$(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g" | grep -o / | wc -l) $(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g")"' \; | grep -v '/jobs/' | sort -u | tr '\n' ',')
-                maxdepth=$(echo ${branches} | tr ',' '\n' | cut -d' ' -f1 | sort -u | tail -n1)
-                leaves=$(echo ${branches} | tr ',' '\n' | grep "^${maxdepth} " | cut -d' ' -f2 | tr '\n' ',')
-                moddirpath=$(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1,2 --complement | rev | head -n1)
+            currdir=${startpath}
 
-                modpath="${moddirpath}/${COMP_WORDS[COMP_CWORD-1]}"
+            module_paths=($(find ${currdir} -mindepth 1 -type d -path '*/.*' -prune -o -type f -name '.DBCrunch' -execdir pwd -P \;))
 
-                modnames=""
-                controllernames=$(find ${modpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev)
+            currdir=$(cd ${currdir}/.. && pwd -P)
+            dirstats=$(stat -c '%a %U' ${currdir})
+            while [[ "${dirstats}" =~ ^"777 " ]] || [[ "${dirstats}" =~ " ${USER}"$ ]]
+            do
+                if [ -f "${currdir}/.DBCrunch" ]
+                then
+                    module_paths=("${currdir}" "${module_paths[@]}")
+                fi
+                currdir=$(cd ${currdir}/.. && pwd -P)
+                dirstats=$(stat -c '%a %U' ${currdir})
+            done
 
-                COMPREPLY=($(compgen -W "${modnames} ${controllernames}" -- ${cur}))
-            else
-                COMPREPLY=($(compgen -W "cancel monitor requeue reset submit template" -- ${cur}))
-            fi            
-            ;;
-        4)
-            if [[ "${COMP_WORDS[COMP_CWORD-3]}" == "--modules_dir" ]]
+            if [ "${#module_paths[@]}" -eq 0 ]
             then
-                modules_dir=$(eval echo ${COMP_WORDS[COMP_CWORD-2]})
-
-                startpath=$(cd ${modules_dir} && pwd -P)
-                branches=$(find ${startpath} -path '*/.*' -prune -o -type d -exec bash -c 'echo "$(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g" | grep -o / | wc -l) $(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g")"' \; | grep -v '/jobs/' | sort -u | tr '\n' ',')
-                maxdepth=$(echo ${branches} | tr ',' '\n' | cut -d' ' -f1 | sort -u | tail -n1)
-                leaves=$(echo ${branches} | tr ',' '\n' | grep "^${maxdepth} " | cut -d' ' -f2 | tr '\n' ',')
-                moddirpath=$(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1,2 --complement | rev | head -n1)
-                #modnames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f2 | rev))
-                #controllernames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1 | rev))
-
-                modcontrollernames=$(find ${moddirpath} -mindepth 1 -path '*/.*' -prune -o -type d -regextype posix-extended -regex ".*/${cur}.*" -print 2>/dev/null | grep -v '/jobs' | rev | cut -d'/' -f1 | rev)
-                
-                COMPREPLY=($(compgen -W "${modcontrollernames}" -- ${cur}))
-            fi
-            ;;
-        5)
-            if [[ "${COMP_WORDS[COMP_CWORD-4]}" == "--modules_dir" ]]
+                echo "Error: No module path detected."
+                exit 1
+            elif [ "${#module_paths[@]}" -gt 1 ]
             then
-                modules_dir=$(eval echo ${COMP_WORDS[COMP_CWORD-3]})
+                echo "Error: Multiple module paths detected. Please choose one:"
+                for i in ${!module_paths[@]}
+                do
+                    echo "  ${i}) ${module_paths[${i}]}"
+                done
+                exit 1
+            fi
 
-                startpath=$(cd ${modules_dir} && pwd -P)
-                branches=$(find ${startpath} -path '*/.*' -prune -o -type d -exec bash -c 'echo "$(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g" | grep -o / | wc -l) $(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g")"' \; | grep -v '/jobs/' | sort -u | tr '\n' ',')
-                maxdepth=$(echo ${branches} | tr ',' '\n' | cut -d' ' -f1 | sort -u | tail -n1)
-                leaves=$(echo ${branches} | tr ',' '\n' | grep "^${maxdepth} " | cut -d' ' -f2 | tr '\n' ',')
-                moddirpath=$(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1,2 --complement | rev | head -n1)
+            moddirpath=${module_paths[0]}
 
-                modpath="${moddirpath}/${COMP_WORDS[COMP_CWORD-1]}"
+            case ${COMP_CWORD} in
+                $((1+${right_shift})) )
+                    COMPREPLY=($(compgen -W "cancel monitor requeue reset submit template test" -- ${cur}))
+                    ;;
+                *)
+                    case ${COMP_WORDS[1+right_shift]} in
+                        test)
+                            right_shift=$((${right_shift}+1))
+                            ;;
+                    esac
+                            
+                    case ${COMP_CWORD} in
+                        $((1+${right_shift})) )
+                            COMPREPLY=($(compgen -W "true false" -- ${cur}))
+                            ;;
+                        $((2+${right_shift})) )
+                            modcontrollernames=$(find ${moddirpath} -mindepth 1 -type d -path '*/.*' -prune -o -type d -regextype posix-extended -regex ".*/${cur}.*" -print 2>/dev/null | grep -v '/jobs' | rev | cut -d'/' -f1 | rev)
+            
+                            COMPREPLY=($(compgen -W "${modcontrollernames}" -- ${cur}))
+                            ;;
+                        $((3+${right_shift})) )
+                            modpath="${moddirpath}/${COMP_WORDS[COMP_CWORD-1]}"
 
-                modnames=""
-                controllernames=$(find ${modpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev)
+                            modnames=""
+                            controllernames=$(find ${modpath} -mindepth 1 -maxdepth 1 -type d -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev)
 
-                COMPREPLY=($(compgen -W "${modnames} ${controllernames}" -- ${cur}))
-            fi
-            ;;
-        *)
-            COMPREPLY=()
+                            COMPREPLY=($(compgen -W "${modnames} ${controllernames}" -- ${cur}))
+                            ;;
+                        *)
+                            COMPREPLY=()
+                            ;;
+                    esac
+                    ;;
+
+            esac
             ;;
     esac
 }
diff --git a/bin/cancel b/bin/cancel
index 9241c55..1b50ccc 100755
--- a/bin/cancel
+++ b/bin/cancel
@@ -1,163 +1,23 @@
 #!/bin/bash
 
-do_cancel () {
-    modpath=$1
-    controllerpath=$2
-    workpath=$3
-
-    jobs=$(squeue -h -u ${USER} -o '%.10i %.100j %.2t' 2>/dev/null | head -c -1 | tr '\n' '!')
-    if [ "$(echo ${jobs} | tr '!' '\n' 2>/dev/null | head -c -1 | grep -vE ' CG( |$)' | grep -v '(null)' | grep "crunch_${modname}_${controllername}_" | wc -l)" -ne 0 ]
-    then
-        while [ "$(echo ${jobs} | tr '!' '\n' 2>/dev/null | head -c -1 | grep -vE ' CG( |$)' | grep -v '(null)' | grep "crunch_${modname}_${controllername}_" | wc -l)" -ne 0 ]
+modpath=$1
+controllerpath=$2
+workpath=$3
+
+modname=$(echo ${modpath} | rev | cut -d'/' -f1 | rev)
+controllername=$(echo ${controllerpath} | rev | cut -d'/' -f1 | rev)
+
+jobs=$(squeue -h -u ${USER} -o '%.10i %.100j %.2t' 2>/dev/null | head -c -1 | tr '\n' '!')
+if [ "$(echo ${jobs} | tr '!' '\n' 2>/dev/null | head -c -1 | grep -vE ' CG( |$)' | grep -v '(null)' | grep "crunch_${modname}_${controllername}_" | wc -l)" -ne 0 ]
+then
+    while [ "$(echo ${jobs} | tr '!' '\n' 2>/dev/null | head -c -1 | grep -vE ' CG( |$)' | grep -v '(null)' | grep "crunch_${modname}_${controllername}_" | wc -l)" -ne 0 ]
+    do
+        for jobid in $(echo " ${jobs}" | tr '!' '\n' 2>/dev/null | grep -vE ' CG( |$)' | grep -v '(null)' | grep "crunch_${modname}_${controllername}_" | sed 's/\s\s*/ /g' | cut -d' ' -f2)
         do
-            for jobid in $(echo " ${jobs}" | tr '!' '\n' 2>/dev/null | grep -vE ' CG( |$)' | grep -v '(null)' | grep "crunch_${modname}_${controllername}_" | sed 's/\s\s*/ /g' | cut -d' ' -f2)
-            do
-                scancel ${jobid} 2>/dev/null
-            done
-            jobs=$(squeue -h -u ${USER} -o '%.10i %.100j %.2t' 2>/dev/null | head -c -1 | tr '\n' '!')
+            scancel ${jobid} 2>/dev/null
         done
+        jobs=$(squeue -h -u ${USER} -o '%.10i %.100j %.2t' 2>/dev/null | head -c -1 | tr '\n' '!')
+    done
 
-        echo "Finished cancelling ${modname}_${controllername}."
-    fi
-}
-
-do_not_dir() {
-    path=$1
-}
-
-modules_dir=$1
-shift
-
-startpath=$(cd ${modules_dir} && pwd -P)
-
-branches=$(find ${startpath} -path '*/.*' -prune -o -type d -exec bash -c 'echo "$(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g" | grep -o / | wc -l) $(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g")"' \; | grep -v '/jobs/' | sort -u | tr '\n' ',')
-maxdepth=$(echo ${branches} | tr ',' '\n' | cut -d' ' -f1 | sort -u | tail -n1)
-leaves=$(echo ${branches} | tr ',' '\n' | grep "^${maxdepth} " | cut -d' ' -f2 | sed 's/\/jobs$//g' | tr '\n' ',')
-moddirpath=$(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1,2 --complement | rev | head -n1)
-modnames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f2 | rev))
-controllernames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1 | rev))
-
-case $# in
-    0)
-        ;;
-    1)
-        if [[ "${moddirpath}" == "${startpath}" ]]
-        then
-            modnames=($1)
-            controllernames=()
-        else
-            stopflag=false
-            for i in ${!controllernames[@]}
-            do
-                if [[ "${controllernames[${i}]}" == "$1" ]]
-                then
-                    modnames=(${modnames[${i}]})
-                    controllernames=(${controllernames[i]})
-                    stopflag=true
-                    break
-                fi
-            done
-            if ! ${stopflag}
-            then
-                for modname in ${modnames[@]}
-                do
-                    if [[ "${modname}" == "$1" ]]
-                    then
-                        modnames=(${modname})
-                        controllernames=()
-                        stopflag=true
-                        break
-                    fi
-                done
-            fi
-            if ! ${stopflag}
-            then
-                for modname in $(find ${moddirpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev)
-                do
-                    if [[ "${modname}" == "$1" ]]
-                    then
-                        modnames=(${modname})
-                        controllernames=()
-                        stopflag=true
-                        break
-                    fi
-                done
-            fi
-            if ! ${stopflag}
-            then
-                for i in ${!modnames[@]}
-                do
-                    modnames=(${modnames[${i}]})
-                    controllernames=($1)
-                done
-            fi
-        fi
-        ;;
-    2)
-        modnames=($1)
-        controllernames=($2)
-        ;;
-    *)
-        echo "Error: You must provide no more than 2 arguments. $# provided."
-        exit
-        ;;
-esac
-
-for i in ${!modnames[@]}
-do
-    modname=${modnames[${i}]}
-
-    modpath="${moddirpath}/${modname}"
-
-    if [ ! -d "${modpath}" ]
-    then
-        do_not_dir ${modpath}
-    fi
-
-    if [[ "${controllernames[${i}]}" == "" ]]
-    then
-        controllernames=($(find ${modpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev))
-
-        for controllername in ${controllernames[@]}
-        do
-            controllerpath="${modpath}/${controllername}"
-
-            if [ ! -d "${controllerpath}" ]
-            then
-                do_not_dir ${controllerpath}
-            fi
-
-            workpath="${controllerpath}/jobs"
-
-            if [ ! -d "${workpath}" ]
-            then
-                do_not_dir ${workpath}
-            fi
-
-            if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-            then
-                do_cancel ${modpath} ${controllerpath} ${workpath}
-            fi
-        done
-    else
-        controllername=${controllernames[${i}]}
-        controllerpath="${modpath}/${controllername}"
-
-        if [ ! -d "${controllerpath}" ]
-        then
-            do_not_dir ${controllerpath}
-        fi
-
-        workpath="${controllerpath}/jobs"
-
-        if [ ! -d "${workpath}" ]
-        then
-            do_not_dir ${workpath}
-        fi
-
-        if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-        then
-            do_cancel ${modpath} ${controllerpath} ${workpath}
-        fi
-    fi
-done
\ No newline at end of file
+    echo "Finished cancelling ${modname}_${controllername}."
+fi
\ No newline at end of file
diff --git a/bin/controller.py b/bin/controller.py
index 6b9b480..1f3de1d 100644
--- a/bin/controller.py
+++ b/bin/controller.py
@@ -8,7 +8,7 @@
 #    the Free Software Foundation, either version 3 of the License, or
 #    (at your option) any later version.
 #
-#    This program is distributed in the hope that it will be useful,
+#    This program is distributed in the hope that it will be useful, 
 #    but WITHOUT ANY WARRANTY; without even the implied warranty of
 #    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 #    GNU General Public License for more details.
@@ -16,1802 +16,1802 @@
 #    You should have received a copy of the GNU General Public License
 #    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 
-import time;
+import time
 #Timer initialization
-starttime=time.time();
+starttime = time.time()
 
-import sys,os,glob,errno,re,linecache,fcntl,traceback,operator,functools,datetime,tempfile,json,yaml;
-from signal import signal,SIGPIPE,SIG_DFL;
-from subprocess import Popen,PIPE;
-from contextlib import contextmanager;
-#from pymongo import MongoClient;
+import sys, os, glob, errno, re, linecache, fcntl, traceback, operator, functools, datetime, tempfile, json, yaml
+from signal import signal, SIGPIPE, SIG_DFL
+from subprocess import Popen, PIPE
+from contextlib import contextmanager
+#from pymongo import MongoClient
 
 #Misc. function definitions
 def PrintException():
     "If an exception is raised, print traceback of it to output log."
-    exc_type, exc_obj, tb = sys.exc_info();
-    f = tb.tb_frame;
-    lineno = tb.tb_lineno;
-    filename = f.f_code.co_filename;
-    linecache.checkcache(filename);
-    line = linecache.getline(filename, lineno, f.f_globals);
-    print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj);
-    print "More info: ",traceback.format_exc();
+    exc_type, exc_obj, tb = sys.exc_info()
+    f = tb.tb_frame
+    lineno = tb.tb_lineno
+    filename = f.f_code.co_filename
+    linecache.checkcache(filename)
+    line = linecache.getline(filename, lineno, f.f_globals)
+    print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)
+    print "More info: ", traceback.format_exc()
 
 def default_sigpipe():
-    signal(SIGPIPE,SIG_DFL);
+    signal(SIGPIPE, SIG_DFL)
 
 def deldup(lst):
     "Delete duplicate elements in lst."
-    return [lst[i] for i in range(len(lst)) if lst[i] not in lst[:i]];
+    return [lst[i] for i in range(len(lst)) if lst[i] not in lst[:i]]
 
-class TimeoutException(Exception): pass;
+class TimeoutException(Exception): pass
 
 @contextmanager
 def time_limit(seconds):
     def signal_handler(signum, frame):
-        raise TimeoutException("Timed out!");
-    signal.signal(signal.SIGALRM, signal_handler);
-    signal.alarm(seconds);
+        raise TimeoutException("Timed out!")
+    signal.signal(signal.SIGALRM, signal_handler)
+    signal.alarm(seconds)
     try:
-        yield;
+        yield
     finally:
-        signal.alarm(0);
+        signal.alarm(0)
 
-def updatereloadstate(reloadstatefilepath,reloadstatefilename,docbatch,endofdocs,filereadform=lambda x:x,filewriteform=lambda x:x,docwriteform=lambda x:x):
+def updatereloadstate(reloadstatefilepath, reloadstatefilename, docbatch, endofdocs, filereadform = lambda x: x, filewriteform = lambda x: x, docwriteform = lambda x: x):
     #Compress docbatch to top tier that has completed
-    endofdocsdone=[];
-    i=len(docbatch)-1;
-    while i>=0:
+    endofdocsdone = []
+    i = len(docbatch)-1
+    while i >= 0:
         if endofdocs[i][1]:
-            endofdocsdone+=[endofdocs[i][0]];
-            with open(reloadstatefilepath+"/"+reloadstatefilename+"FILE","a") as reloadfilesstream:
-                reloadfilesstream.write(filewriteform(endofdocs[i][0])+"\n");
-                reloadfilesstream.flush();
+            endofdocsdone += [endofdocs[i][0]]
+            with open(reloadstatefilepath + "/" + reloadstatefilename + "FILE", "a") as reloadfilesstream:
+                reloadfilesstream.write(filewriteform(endofdocs[i][0]) + "\n")
+                reloadfilesstream.flush()
         else:
             if endofdocs[i][0] not in endofdocsdone:
-                docline="{"+docbatch[i].split("@")[1].split("\n")[0].split(".{")[1].split("<")[0];
-                doc=json.loads(docline);
-                with open(reloadstatefilepath+"/"+reloadstatefilename+"DOC","a") as reloaddocsstream:
-                    reloaddocsstream.write(filewriteform(endofdocs[i][0])+"~"+docwriteform(doc)+"\n");
-                    reloaddocsstream.flush();
-        i-=1;
+                docline = "{" + docbatch[i].split("@")[1].split("\n")[0].split(".{")[1].split("<")[0]
+                doc = json.loads(docline)
+                with open(reloadstatefilepath + "/" + reloadstatefilename + "DOC", "a") as reloaddocsstream:
+                    reloaddocsstream.write(filewriteform(endofdocs[i][0]) + "~" + docwriteform(doc) + "\n")
+                    reloaddocsstream.flush()
+        i -= 1
     #Update reloadstate files by removing the subdocument records of completed documents
     try:
-        with open(reloadstatefilepath+"/"+reloadstatefilename+"DOC","r") as reloaddocsstream, tempfile.NamedTemporaryFile(dir=reloadstatefilepath,delete=False) as tempstream:
+        with open(reloadstatefilepath + "/" + reloadstatefilename + "DOC", "r") as reloaddocsstream, tempfile.NamedTemporaryFile(dir = reloadstatefilepath, delete = False) as tempstream:
             for reloaddocline in reloaddocsstream:
-                fileline=filereadform(reloaddocline.rstrip("\n").split("~")[0]);
+                fileline = filereadform(reloaddocline.rstrip("\n").split("~")[0])
                 #If not a subdocument of any document in the list, keep it
                 if fileline not in endofdocsdone:
-                    tempstream.write(reloaddocline);
-                    tempstream.flush();
-            os.rename(tempstream.name,reloaddocsstream.name);
+                    tempstream.write(reloaddocline)
+                    tempstream.flush()
+            os.rename(tempstream.name, reloaddocsstream.name)
     except IOError:
-        reloaddocsstream=open(reloadstatefilepath+"/"+reloadstatefilename+"DOC","w");
-        reloaddocsstream.close();
+        reloaddocsstream = open(reloadstatefilepath + "/" + reloadstatefilename + "DOC", "w")
+        reloaddocsstream.close()
 
 def printasfunc(*args):
-    docbatch=list(args)[-1];
+    docbatch = list(args)[-1]
     for doc in docbatch:
-        print(doc);
-    sys.stdout.flush();
-    return len(docbatch);
+        print(doc)
+    sys.stdout.flush()
+    return len(docbatch)
 
 def writeasfunc(*args):
-    arglist=list(args);
-    docbatch=arglist[-1];
-    with open(arglist[0],"a") as writestream:
+    arglist = list(args)
+    docbatch = arglist[-1]
+    with open(arglist[0], "a") as writestream:
         for doc in docbatch:
-            writestream.write(doc);
-            writestream.flush();
-        writestream.write("\n");
-        writestream.flush();
-    return len(docbatch);
-
-def reloadcrawl(reloadpath,reloadpattern,reloadstatefilepath,reloadstatefilename="reloadstate",inputfunc=lambda x:{"nsteps":1},inputdoc={"nsteps":1},action=printasfunc,filereadform=lambda x:x,filewriteform=lambda x:x,docwriteform=lambda x:x,timeleft=lambda:1,counters=[1,1],counterupdate=lambda x:None,resetstatefile=False,limit=None):
-    docbatch=[];
-    endofdocs=[];
+            writestream.write(doc)
+            writestream.flush()
+        writestream.write("\n")
+        writestream.flush()
+    return len(docbatch)
+
+def reloadcrawl(reloadpath, reloadpattern, reloadstatefilepath, reloadstatefilename = "reloadstate", inputfunc = lambda x: {"nsteps": 1}, inputdoc = {"nsteps": 1}, action = printasfunc, filereadform = lambda x: x, filewriteform = lambda x: x, docwriteform = lambda x: x, timeleft = lambda: 1, counters = [1, 1], counterupdate = lambda x: None, resetstatefile = False, limit = None):
+    docbatch = []
+    endofdocs = []
     if resetstatefile:
-        for x in ["FILE","DOC"]:
-            loadstatestream=open(reloadstatefilepath+"/"+reloadstatefilename+x,"w");
-            loadstatestream.close();
-    prevfiles=[];
+        for x in ["FILE", "DOC"]:
+            loadstatestream = open(reloadstatefilepath + "/" + reloadstatefilename + x, "w")
+            loadstatestream.close()
+    prevfiles = []
     try:
-        reloadfilesstream=open(reloadstatefilepath+"/"+reloadstatefilename+"FILE","r");
+        reloadfilesstream = open(reloadstatefilepath + "/" + reloadstatefilename + "FILE", "r")
         for fileline in reloadfilesstream:
-            file=filereadform(fileline.rstrip("\n"));
-            prevfiles+=[file];
+            file = filereadform(fileline.rstrip("\n"))
+            prevfiles += [file]
     except IOError:
-        reloadfilesstream=open(reloadstatefilepath+"/"+reloadstatefilename+"FILE","w");
-        pass;
-    reloadfilesstream.close();
-    prevfiledocs=[];
+        reloadfilesstream = open(reloadstatefilepath + "/" + reloadstatefilename + "FILE", "w")
+        pass
+    reloadfilesstream.close()
+    prevfiledocs = []
     try:
-        reloaddocsstream=open(reloadstatefilepath+"/"+reloadstatefilename+"DOC","r");
+        reloaddocsstream = open(reloadstatefilepath + "/" + reloadstatefilename + "DOC", "r")
         for docline in reloaddocsstream:
-            file=filereadform(docline.rstrip("\n").split("~")[0]);
-            doc=docline.rstrip("\n").split("~")[1];
-            prevfiledocs+=[[file,doc]];
+            file = filereadform(docline.rstrip("\n").split("~")[0])
+            doc = docline.rstrip("\n").split("~")[1]
+            prevfiledocs += [[file, doc]]
     except IOError:
-        reloaddocsstream=open(reloadstatefilepath+"/"+reloadstatefilename+"DOC","w");
-        pass;
-    reloaddocsstream.close();
-    if (limit==None) or (counters[1]<=limit):
-        if timeleft()>0:
-            filescurs=glob.iglob(reloadpath+"/"+reloadpattern);
+        reloaddocsstream = open(reloadstatefilepath + "/" + reloadstatefilename + "DOC", "w")
+        pass
+    reloaddocsstream.close()
+    if (limit == None) or (counters[1] <= limit):
+        if timeleft() > 0:
+            filescurs = glob.iglob(reloadpath + "/" + reloadpattern)
         else:
             try:
                 with time_limit(int(timeleft())):
-                    filescurs=glob.iglob(reloadpath+"/"+reloadpattern);
+                    filescurs = glob.iglob(reloadpath + "/" + reloadpattern)
             except TimeoutException(msg):
-                filescurs=iter(());
-                pass;
+                filescurs = iter(())
+                pass
     else:
-        return counters;
-    filepath=next(filescurs,None);
-    while (filepath!=None) and (timeleft()>0) and ((limit==None) or (counters[1]<=limit)):
-        file=filepath.split("/")[-1];
-        #print(file);
-        #sys.stdout.flush();
+        return counters
+    filepath = next(filescurs, None)
+    while (filepath != None) and (timeleft() > 0) and ((limit == None) or (counters[1] <= limit)):
+        file = filepath.split("/")[-1]
+        #print(file)
+        #sys.stdout.flush()
         if file not in prevfiles:
-            docsstream=open(filepath,"r");
-            docsline=docsstream.readline();
-            while (docsline!="") and (timeleft()>0) and ((limit==None) or (counters[1]<=limit)):
-                docsstring="";
-                while (docsline!="") and ("@" not in docsline):
-                    docsstring+=docsline;
-                    docsline=docsstream.readline();
-                #docsline=docsstream.readline();
-                doc=docwriteform(json.loads("{"+docsline.rstrip("\n").split(".{")[1].split("<")[0]));
-                while [file,doc] in prevfiledocs:
-                    #print([file,doc]);
-                    #sys.stdout.flush();
-                    docsline=docsstream.readline();
-                    while (docsline!="") and any([docsline[:len(x)]==x for x in ["CPUTime","MaxRSS","MaxVMSize","BSONSize"]]):
-                        docsline=docsstream.readline();
-                    docsstring="";
-                    while (docsline!="") and ("@" not in docsline):
-                        docsstring+=docsline;
-                        docsline=docsstream.readline();
-                    if docsline=="":
-                        break;
-                    doc=docwriteform(json.loads("{"+docsline.rstrip("\n").split(".{")[1].split("<")[0]));
-                if docsline!="":
-                    #docslinehead=docsline.rstrip("\n").split(".")[0];
-                    #doc=json.loads(".".join(docsline.rstrip("\n").split(".")[1:]));
-                    docsstring+=docsline.rstrip("\n")+"<"+file+"\n";
-                    #print(file+"~"+docsline.rstrip("\n"));
-                    #sys.stdout.flush();
-                    docsline=docsstream.readline();
-                while (docsline!="") and any([docsline[:len(x)]==x for x in ["CPUTime","MaxRSS","MaxVMSize","BSONSize"]]):
-                    docsstring+=docsline;
-                    docsline=docsstream.readline();
-                docbatch+=[docsstring];
-                if docsline=="":
-                    endofdocs+=[[file,True]];
+            docsstream = open(filepath, "r")
+            docsline = docsstream.readline()
+            while (docsline != "") and (timeleft() > 0) and ((limit == None) or (counters[1] <= limit)):
+                docsstring = ""
+                while (docsline != "") and ("@" not in docsline):
+                    docsstring += docsline
+                    docsline = docsstream.readline()
+                #docsline = docsstream.readline()
+                doc = docwriteform(json.loads("{" + docsline.rstrip("\n").split(".{")[1].split("<")[0]))
+                while [file, doc] in prevfiledocs:
+                    #print([file, doc])
+                    #sys.stdout.flush()
+                    docsline = docsstream.readline()
+                    while (docsline != "") and any([docsline[:len(x)] == x for x in ["CPUTime", "MaxRSS", "MaxVMSize", "BSONSize"]]):
+                        docsline = docsstream.readline()
+                    docsstring = ""
+                    while (docsline != "") and ("@" not in docsline):
+                        docsstring += docsline
+                        docsline = docsstream.readline()
+                    if docsline == "":
+                        break
+                    doc = docwriteform(json.loads("{" + docsline.rstrip("\n").split(".{")[1].split("<")[0]))
+                if docsline != "":
+                    #docslinehead = docsline.rstrip("\n").split(".")[0]
+                    #doc = json.loads(".".join(docsline.rstrip("\n").split(".")[1:]))
+                    docsstring += docsline.rstrip("\n") + "<" + file + "\n"
+                    #print(file + "~" + docsline.rstrip("\n"))
+                    #sys.stdout.flush()
+                    docsline = docsstream.readline()
+                while (docsline != "") and any([docsline[:len(x)] == x for x in ["CPUTime", "MaxRSS", "MaxVMSize", "BSONSize"]]):
+                    docsstring += docsline
+                    docsline = docsstream.readline()
+                docbatch += [docsstring]
+                if docsline == "":
+                    endofdocs += [[file, True]]
                 else:
-                    endofdocs+=[[file,False]];
-                if (len(docbatch)==inputdoc["nsteps"]) or not (timeleft()>0):
-                    if (limit!=None) and (counters[1]+len(docbatch)>limit):
-                        docbatch=docbatch[:limit-counters[1]+1];
-                    while len(docbatch)>0:
-                        nextdocind=action(counters,inputdoc,docbatch);
-                        if nextdocind==None:
-                            break;
-                        docbatchpass=docbatch[nextdocind:];
-                        endofdocspass=endofdocs[nextdocind:];
-                        docbatchwrite=docbatch[:nextdocind];
-                        endofdocswrite=endofdocs[:nextdocind];
-                        updatereloadstate(reloadstatefilepath,reloadstatefilename,docbatchwrite,endofdocswrite,filereadform=filereadform,filewriteform=filewriteform,docwriteform=docwriteform);
-                        counters[0]+=1;
-                        counters[1]+=nextdocind;
-                        counterupdate(counters);
-                        docbatch=docbatchpass;
-                        endofdocs=endofdocspass;
-                        inputfuncresult=inputfunc(docbatchpass);
-                        if inputfuncresult==None:
-                            break;
-                        inputdoc.update(inputfuncresult);
-        filepath=next(filescurs,None);
-    while len(docbatch)>0:
-        if (limit!=None) and (counters[1]+len(docbatch)>limit):
-            docbatch=docbatch[:limit-counters[1]+1];
-        nextdocind=action(counters,inputdoc,docbatch);
-        if nextdocind==None:
-            break;
-        docbatchpass=docbatch[nextdocind:];
-        endofdocspass=endofdocs[nextdocind:];
-        docbatchwrite=docbatch[:nextdocind];
-        endofdocswrite=endofdocs[:nextdocind];
-        updatereloadstate(reloadstatefilepath,reloadstatefilename,docbatchwrite,endofdocswrite,filereadform=filereadform,filewriteform=filewriteform,docwriteform=docwriteform);
-        counters[0]+=1;
-        counters[1]+=nextdocind;
-        counterupdate(counters);
-        docbatch=docbatchpass;
-        endofdocs=endofdocspass;
-    return counters;
+                    endofdocs += [[file, False]]
+                if (len(docbatch) == inputdoc["nsteps"]) or not (timeleft() > 0):
+                    if (limit != None) and (counters[1] + len(docbatch) > limit):
+                        docbatch = docbatch[:limit-counters[1] + 1]
+                    while len(docbatch) > 0:
+                        nextdocind = action(counters, inputdoc, docbatch)
+                        if nextdocind == None:
+                            break
+                        docbatchpass = docbatch[nextdocind:]
+                        endofdocspass = endofdocs[nextdocind:]
+                        docbatchwrite = docbatch[:nextdocind]
+                        endofdocswrite = endofdocs[:nextdocind]
+                        updatereloadstate(reloadstatefilepath, reloadstatefilename, docbatchwrite, endofdocswrite, filereadform = filereadform, filewriteform = filewriteform, docwriteform = docwriteform)
+                        counters[0] += 1
+                        counters[1] += nextdocind
+                        counterupdate(counters)
+                        docbatch = docbatchpass
+                        endofdocs = endofdocspass
+                        inputfuncresult = inputfunc(docbatchpass)
+                        if inputfuncresult == None:
+                            break
+                        inputdoc.update(inputfuncresult)
+        filepath = next(filescurs, None)
+    while len(docbatch) > 0:
+        if (limit != None) and (counters[1] + len(docbatch) > limit):
+            docbatch = docbatch[:limit-counters[1] + 1]
+        nextdocind = action(counters, inputdoc, docbatch)
+        if nextdocind == None:
+            break
+        docbatchpass = docbatch[nextdocind:]
+        endofdocspass = endofdocs[nextdocind:]
+        docbatchwrite = docbatch[:nextdocind]
+        endofdocswrite = endofdocs[:nextdocind]
+        updatereloadstate(reloadstatefilepath, reloadstatefilename, docbatchwrite, endofdocswrite, filereadform = filereadform, filewriteform = filewriteform, docwriteform = docwriteform)
+        counters[0] += 1
+        counters[1] += nextdocind
+        counterupdate(counters)
+        docbatch = docbatchpass
+        endofdocs = endofdocspass
+    return counters
 
 '''
 def py2mat(lst):
     "Converts a Python list to a string depicting a list in Mathematica format."
-    return str(lst).replace(" ","").replace("[","{").replace("]","}");
+    return str(lst).replace(" ", "").replace("[", "{").replace("]", "}")
 
 def mat2py(lst):
     "Converts a string depicting a list in Mathematica format to a Python list."
-    return eval(str(lst).replace(" ","").replace("{","[").replace("}","]"));
+    return eval(str(lst).replace(" ", "").replace("{", "[").replace("}", "]"))
 
 def deldup(lst):
     "Delete duplicate elements in lst."
-    return [lst[i] for i in range(len(lst)) if lst[i] not in lst[:i]];
+    return [lst[i] for i in range(len(lst)) if lst[i] not in lst[:i]]
 
 def transpose_list(lst):
     "Get the transpose of a list of lists."
-    return [[lst[i][j] for i in range(len(lst))] for j in range(len(lst[0]))];
+    return [[lst[i][j] for i in range(len(lst))] for j in range(len(lst[0]))]
 
 #Module-specific function definitions
-def collectionfind(db,collection,query,projection):
-    if projection=="Count":
-        result=db[collection].find(query).count();
+def collectionfind(db, collection, query, projection):
+    if projection == "Count":
+        result = db[collection].find(query).count()
     else:
-        result=list(db[collection].find(query,projection));
-    #return [dict(zip(y.keys(),[mat2py(y[x]) for x in y.keys()])) for y in result];
-    return result;
-
-def collectionfieldexists(db,collection,field):
-    result=db[collection].find({},{"_id":0,field:1}).limit(1).next()!={};
-    return result;
-
-def listindexes(db,collection,filters,indexes=["POLYID","GEOMN","TRIANGN","INVOLN"]):
-    trueindexes=[x for x in indexes if collectionfieldexists(db,collection,x)]
-    if len(trueindexes)==0:
-        return [];
-    indexlist=deldup([dict([(x,z[x]) for x in trueindexes if all([x in y.keys() for y in filters])]) for z in filters]);
-    return indexlist;
-
-def sameindexes(filter1,filter2,indexes=["POLYID","GEOMN","TRIANGN","INVOLN"]):
-    return all([filter1[x]==filter2[x] for x in filter1 if (x in indexes) and (x in filter2)]);
-
-def querydatabase(db,queries,tiers=["POLY","GEOM","TRIANG","INVOL"]):
-    sortedprojqueries=sorted([y for y in queries if y[2]!="Count"],key=lambda x: (len(x[1]),tiers.index(x[0])),reverse=True);
-    maxcountquery=[] if len(queries)==len(sortedprojqueries) else [max([y for y in queries if y not in sortedprojqueries],key=lambda x: len(x[1]))];
-    sortedqueries=sortedprojqueries+maxcountquery;
-    totalresult=collectionfind(db,*sortedqueries[0]);
-    if sortedqueries[0][2]=="Count":
-        return totalresult;
-    for i in range(1,len(sortedqueries)):
-        indexlist=listindexes(db,sortedqueries[i][0],totalresult);
-        if len(indexlist)==0:
-            orgroup=sortedqueries[i][1];
+        result = list(db[collection].find(query, projection))
+    #return [dict(zip(y.keys(), [mat2py(y[x]) for x in y.keys()])) for y in result]
+    return result
+
+def collectionfieldexists(db, collection, field):
+    result = db[collection].find({}, {"_id": 0, field: 1}).limit(1).next() != {}
+    return result
+
+def listindexes(db, collection, filters, indexes = ["POLYID", "GEOMN", "TRIANGN", "INVOLN"]):
+    trueindexes = [x for x in indexes if collectionfieldexists(db, collection, x)]
+    if len(trueindexes) == 0:
+        return []
+    indexlist = deldup([dict([(x, z[x]) for x in trueindexes if all([x in y.keys() for y in filters])]) for z in filters])
+    return indexlist
+
+def sameindexes(filter1, filter2, indexes = ["POLYID", "GEOMN", "TRIANGN", "INVOLN"]):
+    return all([filter1[x] == filter2[x] for x in filter1 if (x in indexes) and (x in filter2)])
+
+def querydatabase(db, queries, tiers = ["POLY", "GEOM", "TRIANG", "INVOL"]):
+    sortedprojqueries = sorted([y for y in queries if y[2] != "Count"], key = lambda x: (len(x[1]), tiers.index(x[0])), reverse = True)
+    maxcountquery = [] if len(queries) == len(sortedprojqueries) else [max([y for y in queries if y not in sortedprojqueries], key = lambda x: len(x[1]))]
+    sortedqueries = sortedprojqueries + maxcountquery
+    totalresult = collectionfind(db, *sortedqueries[0])
+    if sortedqueries[0][2] == "Count":
+        return totalresult
+    for i in range(1, len(sortedqueries)):
+        indexlist = listindexes(db, sortedqueries[i][0], totalresult)
+        if len(indexlist) == 0:
+            orgroup = sortedqueries[i][1]
         else:
-            orgroup=dict(sortedqueries[i][1].items()+{"$or":indexlist}.items());
-        nextresult=collectionfind(db,sortedqueries[i][0],orgroup,sortedqueries[i][2]);
-        if sortedqueries[i][2]=="Count":
-            return nextresult;
-        totalresult=[dict(x.items()+y.items()) for x in totalresult for y in nextresult if sameindexes(x,y)];
-    return totalresult;
-
-def querytofile(db,queries,inputpath,inputfile,tiers=["POLY","GEOM","TRIANG","INVOL"]):
-    results=querydatabase(db,queries,tiers);
-    with open(inputpath+"/"+inputfile,"a") as inputstream:
+            orgroup = dict(sortedqueries[i][1].items() + {"$or": indexlist}.items())
+        nextresult = collectionfind(db, sortedqueries[i][0], orgroup, sortedqueries[i][2])
+        if sortedqueries[i][2] == "Count":
+            return nextresult
+        totalresult = [dict(x.items() + y.items()) for x in totalresult for y in nextresult if sameindexes(x, y)]
+    return totalresult
+
+def querytofile(db, queries, inputpath, inputfile, tiers = ["POLY", "GEOM", "TRIANG", "INVOL"]):
+    results = querydatabase(db, queries, tiers)
+    with open(inputpath + "/" + inputfile, "a") as inputstream:
         for doc in results:
-            json.dump(doc,inputstream,separators=(',', ':'));
-            inputstream.write("\n");
-            inputstream.flush();
+            json.dump(doc, inputstream, separators = (', ', ':'))
+            inputstream.write("\n")
+            inputstream.flush()
 
 def py2matdict(dic):
-    return str(dic).replace("u'","'").replace(" ","").replace("'","\\\"").replace(":","->");
+    return str(dic).replace("u'", "'").replace(" ", "").replace("'", "\\\"").replace(":", "->")
 '''
 
-def timestamp2unit(timestamp,unit="seconds"):
-    if timestamp=="infinite":
-        return timestamp;
+def timestamp2unit(timestamp, unit = "seconds"):
+    if timestamp == "infinite":
+        return timestamp
     else:
-        days=0;
+        days = 0
         if "-" in timestamp:
-            daysstr,timestamp=timestamp.split("-");
-            days=int(daysstr);
-        hours,minutes,seconds=[int(x) for x in timestamp.split(":")];
-        hours+=days*24;
-        minutes+=hours*60;
-        seconds+=minutes*60;
-        if unit=="seconds":
-            return seconds;
-        elif unit=="minutes":
-            return float(seconds)/60.;
-        elif unit=="hours":
-            return float(seconds)/(60.*60.);
-        elif unit=="days":
-            return float(seconds)/(60.*60.*24.);
+            daysstr, timestamp = timestamp.split("-")
+            days = int(daysstr)
+        hours, minutes, seconds = [int(x) for x in timestamp.split(":")]
+        hours += days * 24
+        minutes += hours * 60
+        seconds += minutes * 60
+        if unit == "seconds":
+            return seconds
+        elif unit == "minutes":
+            return float(seconds) / 60.
+        elif unit == "hours":
+            return float(seconds) / (60.*60.)
+        elif unit == "days":
+            return float(seconds) / (60.*60.*24.)
         else:
-            return 0;
+            return 0
 
 def seconds2timestamp(seconds):
-    timestamp="";
-    days=str(seconds/(60*60*24));
-    remainder=seconds%(60*60*24);
-    hours=str(remainder/(60*60)).zfill(2);
-    remainder=remainder%(60*60);
-    minutes=str(remainder/60).zfill(2);
-    remainder=remainder%60;
-    seconds=str(remainder).zfill(2);
-    if days!="0":
-        timestamp+=days+"-";
-    timestamp+=hours+":"+minutes+":"+seconds;
-    return timestamp;
-
-#def contractededjobname2jobdocs(jobname,dbindexes):
-#    indexsplit=[[eval(y) for y in x.split("_")] for x in jobname.lstrip("[").rstrip("]").split(",")];
-#    return [dict([(dbindexes[i],x[i]) for i in range(len(dbindexes))]) for x in indexsplit];
-
-#def jobstepname2indexdoc(jobstepname,dbindexes):
-#    indexsplit=jobstepname.split("_");
-#    nindexes=min(len(indexsplit)-2,len(dbindexes));
-#    #return dict([(dbindexes[i],eval(indexsplit[i+2])) for i in range(nindexes)]);
-#    return dict([(dbindexes[i],eval(indexsplit[i+2]) if indexsplit[i+2].isdigit() else indexsplit[i+2]) for i in range(nindexes)]);
-
-#def indexdoc2jobstepname(doc,modname,controllername,dbindexes):
-#    return modname+"_"+controllername+"_"+"_".join([str(doc[x]) for x in dbindexes if x in doc.keys()]);
-
-def indexsplit2indexdoc(indexsplit,dbindexes):
-    nindexes=min(len(indexsplit),len(dbindexes));
-    return dict([(dbindexes[i],eval(indexsplit[i]) if indexsplit[i].isdigit() else indexsplit[i]) for i in range(nindexes)]);
-
-def indexdoc2indexsplit(doc,dbindexes):
-    return [str(doc[x]) for x in dbindexes if x in doc.keys()];
-
-#def doc2jobjson(doc,dbindexes):
-#    return dict([(y,doc[y]) for y in dbindexes]);
+    timestamp = ""
+    days = str(seconds / (60 * 60 * 24))
+    remainder = seconds % (60 * 60 * 24)
+    hours = str(remainder / (60 * 60)).zfill(2)
+    remainder = remainder % (60 * 60)
+    minutes = str(remainder / 60).zfill(2)
+    remainder = remainder % 60
+    seconds = str(remainder).zfill(2)
+    if days != "0":
+        timestamp += days + "-"
+    timestamp += hours + ":" + minutes + ":" + seconds
+    return timestamp
+
+#def contractededjobname2jobdocs(jobname, dbindexes):
+#    indexsplit = [[eval(y) for y in x.split("_")] for x in jobname.lstrip("[").rstrip("]").split(",")]
+#    return [dict([(dbindexes[i], x[i]) for i in range(len(dbindexes))]) for x in indexsplit]
+
+#def jobstepname2indexdoc(jobstepname, dbindexes):
+#    indexsplit = jobstepname.split("_")
+#    nindexes = min(len(indexsplit)-2, len(dbindexes))
+#    #return dict([(dbindexes[i], eval(indexsplit[i + 2])) for i in range(nindexes)])
+#    return dict([(dbindexes[i], eval(indexsplit[i + 2]) if indexsplit[i + 2].isdigit() else indexsplit[i + 2]) for i in range(nindexes)])
+
+#def indexdoc2jobstepname(doc, modname, controllername, dbindexes):
+#    return modname + "_" + controllername + "_" + "_".join([str(doc[x]) for x in dbindexes if x in doc.keys()])
+
+def indexsplit2indexdoc(indexsplit, dbindexes):
+    nindexes = min(len(indexsplit), len(dbindexes))
+    return dict([(dbindexes[i], eval(indexsplit[i]) if indexsplit[i].isdigit() else indexsplit[i]) for i in range(nindexes)])
+
+def indexdoc2indexsplit(doc, dbindexes):
+    return [str(doc[x]) for x in dbindexes if x in doc.keys()]
+
+#def doc2jobjson(doc, dbindexes):
+#    return dict([(y, doc[y]) for y in dbindexes])
 
 #def jobnameexpand(jobname):
-#    bracketexpanded=jobname.rstrip("]").split("[");
-#    return [bracketexpanded[0]+x for x in bracketexpanded[1].split(",")];
+#    bracketexpanded = jobname.rstrip("]").split("[")
+#    return [bracketexpanded[0] + x for x in bracketexpanded[1].split(",")]
 
 #def jobstepnamescontract(jobstepnames):
-#    "3 because modname,controllername are first two."
-#    bracketcontracted=[x.split("_") for x in jobstepnames];
-#    return '_'.join(bracketcontracted[0][:-3]+["["])+','.join(['_'.join(x[-3:]) for x in bracketcontracted])+"]";
-
-#def formatinput(doc):#,scriptlanguage):
-#    #if scriptlanguage=="python":
-#    #    formatteddoc=doc;
-#    #elif scriptlanguage=="sage":
-#    #    formatteddoc=doc;
-#    #elif scriptlanguage=="mathematica":
-#    #    formatteddoc=mongolink.pythondictionary2mathematicarules(doc);
-#    #return str(formatteddoc).replace(" ","");
-#    return json.dumps(doc,separators=(',',':')).replace("\"","\\\"");
-
-def dir_size(start_path='.'):
-    total_size=0;
-    for dirpath,dirnames,filenames in os.walk(start_path):
+#    "3 because modname, controllername are first two."
+#    bracketcontracted = [x.split("_") for x in jobstepnames]
+#    return '_'.join(bracketcontracted[0][:-3] + ["["]) + ', '.join(['_'.join(x[-3:]) for x in bracketcontracted]) + "]"
+
+#def formatinput(doc):#, scriptlanguage):
+#    #if scriptlanguage == "python":
+#    #    formatteddoc = doc
+#    #elif scriptlanguage == "sage":
+#    #    formatteddoc = doc
+#    #elif scriptlanguage == "mathematica":
+#    #    formatteddoc = mongojoin.pythondictionary2mathematicarules(doc)
+#    #return str(formatteddoc).replace(" ", "")
+#    return json.dumps(doc, separators = (',',':')).replace("\"", "\\\"")
+
+def dir_size(start_path = '.'):
+    total_size = 0
+    for dirpath, dirnames, filenames in os.walk(start_path):
         for f in filenames:
-            fp=os.path.join(dirpath,f);
+            fp = os.path.join(dirpath, f)
             try:
-                total_size+=os.stat(fp).st_blocks*512;
+                total_size += os.stat(fp).st_blocks * 512
             except OSError:
-                pass;
-    return total_size;
+                pass
+    return total_size
 
-def storageleft(path,storagelimit):
-    if storagelimit==None:
-        return True;
+def storageleft(path, storagelimit):
+    if storagelimit == None:
+        return True
     else:
-        return dir_size(path)<storagelimit;
-
-def getpartitiontimelimit(partition,scripttimelimit,scriptbuffertime):
-    maxtimelimit=get_maxtimelimit(partition);
-    #print "getpartitiontimelimit";
-    #print "sinfo -h -o '%l %P' | grep -E '"+partition+"\*?\s*$' | sed 's/\s\s*/ /g' | sed 's/*//g' | cut -d' ' -f1 | head -c -1";
-    #print "";
-    #sys.stdout.flush();
-    if scripttimelimit in ["","infinite"]:
-        partitiontimelimit=maxtimelimit;
+        return dir_size(path) < storagelimit
+
+def getpartitiontimelimit(partition, scripttimelimit, scriptbuffertime):
+    maxtimelimit = get_maxtimelimit(partition)
+    #print "getpartitiontimelimit"
+    #print "sinfo -h -o '%l %P' | grep -E '" + partition + "\*?\s*$' | sed 's/\s\s*/ /g' | sed 's/*//g' | cut -d' ' -f1 | head -c -1"
+    #print ""
+    #sys.stdout.flush()
+    if scripttimelimit in ["", "infinite"]:
+        partitiontimelimit = maxtimelimit
     else:
-        if maxtimelimit=="infinite":
-            partitiontimelimit=scripttimelimit;
+        if maxtimelimit == "infinite":
+            partitiontimelimit = scripttimelimit
         else:
-            partitiontimelimit=min(maxtimelimit,scripttimelimit,key=timestamp2unit);
-    if partitiontimelimit=="infinite":
-        buffertimelimit=partitiontimelimit;
+            partitiontimelimit = min(maxtimelimit, scripttimelimit, key = timestamp2unit)
+    if partitiontimelimit == "infinite":
+        buffertimelimit = partitiontimelimit
     else:
-        buffertimelimit=seconds2timestamp(timestamp2unit(partitiontimelimit)-timestamp2unit(scriptbuffertime));
-    return [partitiontimelimit,buffertimelimit];
+        buffertimelimit = seconds2timestamp(timestamp2unit(partitiontimelimit)-timestamp2unit(scriptbuffertime))
+    return [partitiontimelimit, buffertimelimit]
 
-def timeleft(starttime,buffertimelimit):
+def timeleft(starttime, buffertimelimit):
     "Determine if runtime limit has been reached."
-    #print str(time.time()-starttime)+" "+str(timestamp2unit(buffertimelimit));
-    #sys.stdout.flush();
-    if buffertimelimit=="infinite":
-        return 1;
+    #print str(time.time()-starttime) + " " + str(timestamp2unit(buffertimelimit))
+    #sys.stdout.flush()
+    if buffertimelimit == "infinite":
+        return 1
     else:
-        return timestamp2unit(buffertimelimit)-(time.time()-starttime);
+        return timestamp2unit(buffertimelimit)-(time.time()-starttime)
 
-#def timeleftq(controllerjobid,buffertimelimit):
+#def timeleftq(controllerjobid, buffertimelimit):
 #    "Determine if runtime limit has been reached."
-#    if buffertimelimit=="infinite":
-#        return True;
+#    if buffertimelimit == "infinite":
+#        return True
 #    else:
-#        timestats=Popen("sacct -n -j \""+controllerjobid+"\" -o 'Elapsed,Timelimit' | head -n1 | sed 's/^\s*//g' | sed 's/\s\s*/ /g' | tr ' ' ',' | tr '\n' ',' | head -c -2",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0];
-#        elapsedtime,timelimit=timestats.split(",");
-#        return timestamp2unit(elapsedtime)<timestamp2unit(buffertimelimit);
+#        timestats = Popen("sacct -n -j \"" + controllerjobid + "\" -o 'Elapsed,Timelimit' | head -n1 | sed 's/^\s*//g' | sed 's/\s\s*/ /g' | tr ' ' ',' | tr '\n' ',' | head -c -2", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0]
+#        elapsedtime, timelimit = timestats.split(",")
+#        return timestamp2unit(elapsedtime) < timestamp2unit(buffertimelimit)
 
 #def clusterjobslotsleft(globalmaxjobcount):
-#    njobs=eval(Popen("squeue -h -r | wc -l",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);
-#    return njobs<globalmaxjobcount;
+#    njobs = eval(Popen("squeue -h -r | wc -l", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
+#    return njobs < globalmaxjobcount
 
-def availlicensecount(localbinpath,licensescript,sublicensescript):
-    if licensescript!=None and os.path.isfile(licensescript):
-        navaillicenses=[eval(Popen(licensescript,shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0])];
+def availlicensecount(localbinpath, licensescript, sublicensescript):
+    if licensescript != None and os.path.isfile(licensescript):
+        navaillicenses = [eval(Popen(licensescript, shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])]
     else:
-        raise IOError(errno.ENOENT,'No license script available.',licensescript);
-    if sublicensescript!=None and os.path.isfile(sublicensescript):
-        navaillicenses+=[eval(Popen(sublicensescript,shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0])];
-    #print "licensecount";
-    #print binpath+"/"+scriptlanguage+"licensecount.bash";
-    #print "";
-    #sys.stdout.flush();
-    return navaillicenses;
-
-def pendlicensecount(username,needslicense):
-    npendjobsteps=0;
-    npendjobthreads=0;
-    #grepmods="|".join(modlist);
-    #pendjobnamespaths=Popen("squeue -h -u "+username+" -o '%T %j %.130Z' | grep 'PENDING' | cut -d' ' -f2,3 | grep -E \"("+grepmods+")\" | grep -v \"controller\" | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0];
-    pendjobnamespaths=get_pendjobnamespaths(username);
-    if len(pendjobnamespaths)>0:
-        for pendjobname,pendjobpath in pendjobnamespaths:
-            pendjobnamesplit=pendjobname.split("_");
-            modname=pendjobnamesplit[0];
-            controllername=pendjobnamesplit[1];
-            if os.path.exists(pendjobpath+"/../crunch_"+modname+"_"+controllername+"_controller.job"):
-                nsteps=1-eval(pendjobnamesplit[5]);
-                njobthreads=eval(Popen("echo \"$(cat "+pendjobpath+"/"+pendjobname+".job | grep -E \"njobstepthreads\[[0-9]+\]=\" | cut -d'=' -f2 | tr '\n' '+' | head -c -1)\" | bc | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);
+        raise IOError(errno.ENOENT, 'No license script available.', licensescript)
+    if sublicensescript != None and os.path.isfile(sublicensescript):
+        navaillicenses += [eval(Popen(sublicensescript, shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])]
+    #print "licensecount"
+    #print binpath + "/" + scriptlanguage + "licensecount.bash"
+    #print ""
+    #sys.stdout.flush()
+    return navaillicenses
+
+def pendlicensecount(username, needslicense):
+    npendjobsteps = 0
+    npendjobthreads = 0
+    #grepmods = "|".join(modlist)
+    #pendjobnamespaths = Popen("squeue -h -u " + username + " -o '%T %j %.130Z' | grep 'PENDING' | cut -d' ' -f2,3 | grep -E \"(" + grepmods + ")\" | grep -v \"controller\" | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0]
+    pendjobnamespaths = get_pendjobnamespaths(username)
+    if len(pendjobnamespaths) > 0:
+        for pendjobname, pendjobpath in pendjobnamespaths:
+            pendjobnamesplit = pendjobname.split("_")
+            modname = pendjobnamesplit[0]
+            controllername = pendjobnamesplit[1]
+            if os.path.exists(pendjobpath + "/../crunch_" + modname + "_" + controllername + "_controller.job"):
+                nsteps = 1-eval(pendjobnamesplit[5])
+                njobthreads = eval(Popen("echo \"$(cat " + pendjobpath + "/" + pendjobname + ".job | grep -E \"njobstepthreads\[[0-9]+\]=\" | cut -d'=' -f2 | tr '\n' '+' | head -c -1)\" | bc | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
                 if needslicense:
-                    npendjobsteps+=nsteps;
-                    npendjobthreads+=njobthreads;
-    return [npendjobsteps,npendjobthreads];
+                    npendjobsteps += nsteps
+                    npendjobthreads += njobthreads
+    return [npendjobsteps, npendjobthreads]
 
-def licensecount(username,needslicense,localbinpath,licensescript,sublicensescript):
-    navaillicensesplit=availlicensecount(localbinpath,licensescript,sublicensescript);
-    npendlicensesplit=pendlicensecount(username,needslicense);
+def licensecount(username, needslicense, localbinpath, licensescript, sublicensescript):
+    navaillicensesplit = availlicensecount(localbinpath, licensescript, sublicensescript)
+    npendlicensesplit = pendlicensecount(username, needslicense)
     try:
-        nlicensesplit=[navaillicensesplit[i]-npendlicensesplit[i] for i in range(len(navaillicensesplit))];
+        nlicensesplit = [navaillicensesplit[i]-npendlicensesplit[i] for i in range(len(navaillicensesplit))]
     except IndexError:
-        raise;
-    return nlicensesplit;
-
-def clusterjobslotsleft(username,modname,controllername,globalmaxjobcount,localmaxjobcount):
-    nglobaljobs=get_nglobaljobs(username);
-    globaljobsleft=(nglobaljobs<globalmaxjobcount);
-    nlocaljobs=get_nlocaljobs(username,modname,controllername);
-    localjobsleft=(nlocaljobs<localmaxjobcount);
-    return (globaljobsleft and localjobsleft);
-
-def clusterlicensesleft(nlicensesplit,minthreads):#,minnsteps=1):
-    nlicenses=nlicensesplit[0];
-    licensesleft=(nlicenses>0);#(navaillicenses>=minnsteps));
-    if len(nlicensesplit)>1:
-        nsublicenses=nlicensesplit[1];
-        licensesleft=(licensesleft and (nsublicenses>=minthreads));
-    return licensesleft;
-
-#def islimitreached(controllerpath,querylimit):
-#    if querylimit==None:
-#        return False;
+        raise
+    return nlicensesplit
+
+def clusterjobslotsleft(username, modname, controllername, globalmaxjobcount, localmaxjobcount):
+    nglobaljobs = get_nglobaljobs(username)
+    globaljobsleft = (nglobaljobs < globalmaxjobcount)
+    nlocaljobs = get_nlocaljobs(username, modname, controllername)
+    localjobsleft = (nlocaljobs < localmaxjobcount)
+    return (globaljobsleft and localjobsleft)
+
+def clusterlicensesleft(nlicensesplit, minthreads):#, minnsteps = 1):
+    nlicenses = nlicensesplit[0]
+    licensesleft = (nlicenses > 0);#(navaillicenses >= minnsteps))
+    if len(nlicensesplit) > 1:
+        nsublicenses = nlicensesplit[1]
+        licensesleft = (licensesleft and (nsublicenses >= minthreads))
+    return licensesleft
+
+#def islimitreached(controllerpath, querylimit):
+#    if querylimit == None:
+#        return False
 #    else:
-#        #if niters==1:
-#        #    ntot=eval(Popen("echo \"$(cat "+controllerpath+"/jobs/*.error 2>/dev/null | wc -l)+$(cat "+controllerpath+"/jobs/*.job.log 2>/dev/null | grep 'CPUTime' | wc -l)\" | bc | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);
-#        #elif niters>1:
-#        ntot=eval(Popen("echo \"$(cat $(find "+controllerpath+"/jobs/ -type f -name '*.docs' -o -name '*.docs.pend' 2>/dev/null) 2>/dev/null | wc -l)+$(cat "+controllerpath+"/jobs/*.job.log 2>/dev/null | grep 'CPUTime' | wc -l)\" | bc | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);
-#        return ntot>=querylimit;
-
-def submitjob(jobpath,jobname,jobstepnames,nnodes,ncores,nthreads,niters,nbatch,partition,memoryperstep,maxmemorypernode,resubmit=False):
-    jobid=get_submitjob(jobpath,jobname);
+#        #if niters == 1:
+#        #    ntot = eval(Popen("echo \"$(cat " + controllerpath + "/jobs/*.error 2>/dev/null | wc -l)+$(cat " + controllerpath + "/jobs/*.job.log 2>/dev/null | grep 'CPUTime' | wc -l)\" | bc | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
+#        #elif niters > 1:
+#        ntot = eval(Popen("echo \"$(cat $(find " + controllerpath + "/jobs/ -type f -name '*.docs' -o -name '*.docs.pend' 2>/dev/null) 2>/dev/null | wc -l)+$(cat " + controllerpath + "/jobs/*.job.log 2>/dev/null | grep 'CPUTime' | wc -l)\" | bc | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
+#        return ntot >= querylimit
+
+def submitjob(jobpath, jobname, jobstepnames, nnodes, ncores, nthreads, niters, nbatch, partition, memoryperstep, maxmemorypernode, resubmit = False):
+    jobid = get_submitjob(jobpath, jobname)
     #Print information about controller job submission
     if resubmit:
-        #jobid=submitcomm.split(' ')[-1];
-        #maketop=Popen("scontrol top "+jobid,shell=True,stdout=PIPE,preexec_fn=default_sigpipe);
-        print "";
-        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
-        print "Resubmitted batch job "+jobid+" as "+jobname+" on partition "+partition+" with "+str(nnodes)+" nodes, "+str(ncores)+" CPU(s), and "+str(maxmemorypernode/1000000)+"MB RAM allocated.";
+        #jobid = submitcomm.split(' ')[-1]
+        #maketop = Popen("scontrol top " + jobid, shell = True, stdout = PIPE, preexec_fn = default_sigpipe)
+        print ""
+        print datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")
+        print "Resubmitted batch job " + jobid + " as " + jobname + " on partition " + partition + " with " + str(nnodes) + " nodes, " + str(ncores) + " CPU(s), and " + str(maxmemorypernode / 1000000) + "MB RAM allocated."
         for jobstepnum in range(len(jobstepnames)):
-            #with open(jobpath+"/"+jobstepnames[i]+".error","a") as statstream:
-            #    statstream.write(jobstepnames[i]+",-1:0,False\n");
-            #    statstream.flush();
-            print "....With job step "+jobid+"."+str(jobstepnum)+" as "+jobstepnames[i]+" in batches of "+str(nbatch)+"/"+str(niters)+" iteration(s) on partition "+partition+" with "+str(nthreads[jobstepnum])+" CPU(s) and "+str(memoryperstep/1000000)+"MB RAM allocated.";
-        print "";
-        print "";
+            #with open(jobpath + "/" + jobstepnames[i] + ".error", "a") as statstream:
+            #    statstream.write(jobstepnames[i] + ", -1:0, False\n")
+            #    statstream.flush()
+            print "....With job step " + jobid + "." + str(jobstepnum) + " as " + jobstepnames[i] + " in batches of " + str(nbatch) + "/" + str(niters) + " iteration(s) on partition " + partition + " with " + str(nthreads[jobstepnum]) + " CPU(s) and " + str(memoryperstep / 1000000) + "MB RAM allocated."
+        print ""
+        print ""
     else:
-        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
-        print "Submitted batch job "+jobid+" as "+jobname+" on partition "+partition+" with "+str(nnodes)+" nodes, "+str(ncores)+" CPU(s), and "+str(maxmemorypernode/1000000)+"MB RAM allocated.";
+        print datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")
+        print "Submitted batch job " + jobid + " as " + jobname + " on partition " + partition + " with " + str(nnodes) + " nodes, " + str(ncores) + " CPU(s), and " + str(maxmemorypernode / 1000000) + "MB RAM allocated."
         for jobstepnum in range(len(jobstepnames)):
-            #with open(jobpath+"/"+jobstepnames[i]+".error","a") as statstream:
-            #    statstream.write(jobstepnames[i]+",-1:0,False\n");
-            #    statstream.flush();
-            print "....With job step "+jobid+"."+str(jobstepnum)+" as "+jobstepnames[jobstepnum]+" in batches of "+str(nbatch)+"/"+str(niters)+" iteration(s) on partition "+partition+" with "+str(nthreads[jobstepnum])+" CPU(s) and "+str(memoryperstep/1000000)+"MB RAM allocated.";
-        print "";
-    sys.stdout.flush();
-
-def submitcontrollerjob(jobpath,jobname,controllernnodes,controllerncores,partition,maxmemorypernode,resubmit=False):
-    jobid=get_submitjob(jobpath,jobname);
+            #with open(jobpath + "/" + jobstepnames[i] + ".error", "a") as statstream:
+            #    statstream.write(jobstepnames[i] + ", -1:0, False\n")
+            #    statstream.flush()
+            print "....With job step " + jobid + "." + str(jobstepnum) + " as " + jobstepnames[jobstepnum] + " in batches of " + str(nbatch) + "/" + str(niters) + " iteration(s) on partition " + partition + " with " + str(nthreads[jobstepnum]) + " CPU(s) and " + str(memoryperstep / 1000000) + "MB RAM allocated."
+        print ""
+    sys.stdout.flush()
+
+def submitcontrollerjob(jobpath, jobname, controllernnodes, controllerncores, partition, maxmemorypernode, resubmit = False):
+    jobid = get_submitjob(jobpath, jobname)
     #Print information about controller job submission
     if resubmit:
-        #jobid=submitcomm.split(' ')[-1];
-        #maketop=Popen("scontrol top "+jobid,shell=True,stdout=PIPE,preexec_fn=default_sigpipe);
-        print "";
-        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
-        print "Resubmitted batch job "+jobid+" as "+jobname+" on partition "+partition+" with "+controllernnodes+" nodes, "+controllerncores+" CPU(s), and "+str(maxmemorypernode/1000000)+"MB RAM allocated.";
-        print "";
-        print "";
+        #jobid = submitcomm.split(' ')[-1]
+        #maketop = Popen("scontrol top " + jobid, shell = True, stdout = PIPE, preexec_fn = default_sigpipe)
+        print ""
+        print datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")
+        print "Resubmitted batch job " + jobid + " as " + jobname + " on partition " + partition + " with " + controllernnodes + " nodes, " + controllerncores + " CPU(s), and " + str(maxmemorypernode / 1000000) + "MB RAM allocated."
+        print ""
+        print ""
     else:
-        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
-        print "Submitted batch job "+jobid+" as "+jobname+" on partition "+partition+" with "+controllernnodes+" nodes, "+controllerncores+" CPU(s), and "+str(maxmemorypernode/1000000)+"MB RAM allocated.";
-        print "";
-    sys.stdout.flush();
-
-#def skippedjobslist(username,modname,controllername,workpath):
-#    jobsrunning=userjobsrunninglist(username,modname,controllername);
-#    blankfilesstring=Popen("find '"+workpath+"' -maxdepth 1 -type f -name '*.out' -empty | rev | cut -d'/' -f1 | rev | cut -d'.' -f1 | cut -d'_' -f1,2 --complement | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0];
-#    if blankfilesstring=='':
-#        return [];
+        print datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")
+        print "Submitted batch job " + jobid + " as " + jobname + " on partition " + partition + " with " + controllernnodes + " nodes, " + controllerncores + " CPU(s), and " + str(maxmemorypernode / 1000000) + "MB RAM allocated."
+        print ""
+    sys.stdout.flush()
+
+#def skippedjobslist(username, modname, controllername, workpath):
+#    jobsrunning = userjobsrunninglist(username, modname, controllername)
+#    blankfilesstring = Popen("find '" + workpath + "' -maxdepth 1 -type f -name '*.out' -empty | rev | cut -d'/' -f1 | rev | cut -d'.' -f1 | cut -d'_' -f1,2 --complement | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0]
+#    if blankfilesstring == '':
+#        return []
 #    else:
-#        blankfiles=blankfilesstring.split(",");
-#        skippedjobs=[modname+"_"+controllername+"_"+x for x in blankfiles if x not in jobsrunning];
-#        return skippedjobs;
+#        blankfiles = blankfilesstring.split(",")
+#        skippedjobs = [modname + "_" + controllername + "_" + x for x in blankfiles if x not in jobsrunning]
+#        return skippedjobs
 
-def requeueskippedqueryjobs(modname,controllername,controllerpath,querystatefilename,basecollection,counters,counterstatefile,counterheader,dbindexes):
+def requeueskippedqueryjobs(modname, controllername, controllerpath, querystatefilename, basecollection, counters, counterstatefile, counterheader, dbindexes):
     try:
-        skippeddoccount=0;
-        skippedjobfiles=[];
-        skippedjobnums=[];
-        skippedjobdocs=[];
-        with open(controllerpath+"/skipped","r") as skippedstream:#, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream:
-            skippedheader=skippedstream.readline();
-            #tempstream.write(skippedheader);
-            #tempstream.flush();
+        skippeddoccount = 0
+        skippedjobfiles = []
+        skippedjobnums = []
+        skippedjobdocs = []
+        with open(controllerpath + "/skipped", "r") as skippedstream:#, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream:
+            skippedheader = skippedstream.readline()
+            #tempstream.write(skippedheader)
+            #tempstream.flush()
             for line in skippedstream:
-                skippedjobfile,exitcode,resubmitq=line.rstrip("\n").split(",");
+                skippedjobfile, exitcode, resubmitq = line.rstrip("\n").split(",")
                 if eval(resubmitq):
-                    skippedjobfilesplit=skippedjobfile.split("_");
-                    if skippedjobfilesplit[:2]==[modname,controllername]:
-                        #if niters==1:
-                        skippedjobnums+=[re.sub(".*_job_([0-9]+)[_\.].*",r"\1",skippedjobfile)];
-                        skippedjobfiles+=[skippedjobfile];
-                        #elif niters>1:
-                        skippedjobfiledocs=[];
-                        with open(controllerpath+"/jobs/"+skippedjobfile,"r") as skippeddocstream:
+                    skippedjobfilesplit = skippedjobfile.split("_")
+                    if skippedjobfilesplit[:2] == [modname, controllername]:
+                        #if niters == 1:
+                        skippedjobnums += [re.sub(".*_job_([0-9] + )[_\.].*", r"\1", skippedjobfile)]
+                        skippedjobfiles += [skippedjobfile]
+                        #elif niters > 1:
+                        skippedjobfiledocs = []
+                        with open(controllerpath + "/jobs/" + skippedjobfile, "r") as skippeddocstream:
                             for docsline in skippeddocstream:
-                                doc=json.loads(docsline.rstrip("\n"));
-                                skippedjobfiledocs+=[modname+"_"+controllername+"_"+"_".join(indexdoc2indexsplit(doc,dbindexes))];
-                                skippeddoccount+=1;
-                        skippedjobdocs+=[skippedjobfiledocs];
-                        #os.remove(controllerpath+"/jobs/"+skippedjob+".docs");
-                        #os.remove(controllerpath+"/jobs/"+skippedjob+".docs.in");
-                        #os.remove(controllerpath+"/jobs/"+skippedjob+".error");
-                        #os.remove(controllerpath+"/jobs/"+skippedjob+".batch.log");
+                                doc = json.loads(docsline.rstrip("\n"))
+                                skippedjobfiledocs += [modname + "_" + controllername + "_" + "_".join(indexdoc2indexsplit(doc, dbindexes))]
+                                skippeddoccount += 1
+                        skippedjobdocs += [skippedjobfiledocs]
+                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".docs")
+                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".docs.in")
+                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".error")
+                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".batch.log")
                     #else:
-                    #    tempstream.write(line);
-                    #    tempstream.flush();
+                    #    tempstream.write(line)
+                    #    tempstream.flush()
                 #else:
-                #    tempstream.write(line);
-                #    tempstream.flush();
-            #os.rename(tempstream.name,skippedstream.name);
-        if skippeddoccount>0:
-            querystatetierfilenames=Popen("find "+controllerpath+"/ -maxdepth 1 -type f -name '"+querystatefilename+"*' 2>/dev/null | rev | cut -d'/' -f1 | rev | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(",");
+                #    tempstream.write(line)
+                #    tempstream.flush()
+            #os.rename(tempstream.name, skippedstream.name)
+        if skippeddoccount > 0:
+            querystatetierfilenames = Popen("find " + controllerpath + "/ -maxdepth 1 -type f -name '" + querystatefilename + "*' 2>/dev/null | rev | cut -d'/' -f1 | rev | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(",")
             for querystatetierfilename in querystatetierfilenames:
                 try:
-                    if querystatetierfilename==querystatefilename+basecollection:
-                        #with open(controllerpath+"/"+querystatetierfilename,"a") as querystatefilestream:
+                    if querystatetierfilename == querystatefilename + basecollection:
+                        #with open(controllerpath + "/" + querystatetierfilename, "a") as querystatefilestream:
                         #    for i in range(len(skippedjobs)):
-                        #        line=skippedjobs[i];
-                        #        if i<len(skippedjobs)-1:
-                        #            line+="\n";
-                        #        querystatefilestream.write(line);
-                        #        querystatefilestream.flush();
-                        with open(controllerpath+"/"+querystatetierfilename,"r") as querystatefilestream, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream1:
-                            #print(skippedjobdocs);
-                            #print("");
-                            #sys.stdout.flush();
+                        #        line = skippedjobs[i]
+                        #        if i < len(skippedjobs)-1:
+                        #            line += "\n"
+                        #        querystatefilestream.write(line)
+                        #        querystatefilestream.flush()
+                        with open(controllerpath + "/" + querystatetierfilename, "r") as querystatefilestream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream1:
+                            #print(skippedjobdocs)
+                            #print("")
+                            #sys.stdout.flush()
                             for querystateline in querystatefilestream:
-                                querystatelinestrip=querystateline.rstrip("\n");
-                                skipped=False;
-                                i=0;
-                                #print(linestrip);
-                                #sys.stdout.flush();
-                                while i<len(skippedjobdocs):
+                                querystatelinestrip = querystateline.rstrip("\n")
+                                skipped = False
+                                i = 0
+                                #print(linestrip)
+                                #sys.stdout.flush()
+                                while i < len(skippedjobdocs):
                                     if querystatelinestrip in skippedjobdocs[i]:
-                                        skippedjobdocs[i].remove(querystatelinestrip);
-                                        if len(skippedjobdocs[i])==0:
-                                            skippedjobfile=skippedjobfiles[i];
-                                            skippedjobnum=skippedjobnums[i];
-                                            if not os.path.isdir(controllerpath+"/jobs/reloaded"):
-                                                os.mkdir(controllerpath+"/jobs/reloaded");
-                                            os.rename(controllerpath+"/jobs/"+skippedjobfile,controllerpath+"/jobs/reloaded/"+skippedjobfile);
-                                            del skippedjobdocs[i];
-                                            del skippedjobfiles[i];
-                                            del skippedjobnums[i];
+                                        skippedjobdocs[i].remove(querystatelinestrip)
+                                        if len(skippedjobdocs[i]) == 0:
+                                            skippedjobfile = skippedjobfiles[i]
+                                            skippedjobnum = skippedjobnums[i]
+                                            if not os.path.isdir(controllerpath + "/jobs/reloaded"):
+                                                os.mkdir(controllerpath + "/jobs/reloaded")
+                                            os.rename(controllerpath + "/jobs/" + skippedjobfile, controllerpath + "/jobs/reloaded/" + skippedjobfile)
+                                            del skippedjobdocs[i]
+                                            del skippedjobfiles[i]
+                                            del skippedjobnums[i]
                                             try:
-                                                skippedjobfilein=skippedjobfile.replace(".docs",".in");
-                                                os.rename(controllerpath+"/jobs/"+skippedjobfilein,controllerpath+"/jobs/reloaded/"+skippedjobfilein);
+                                                skippedjobfilein = skippedjobfile.replace(".docs", ".in")
+                                                os.rename(controllerpath + "/jobs/" + skippedjobfilein, controllerpath + "/jobs/reloaded/" + skippedjobfilein)
                                             except:
-                                                pass;
+                                                pass
                                             try:
-                                                skippedjobfiletemp=skippedjobfile.replace(".docs",".temp");
-                                                os.rename(controllerpath+"/jobs/"+skippedjobfiletemp,controllerpath+"/jobs/reloaded/"+skippedjobfiletemp);
+                                                skippedjobfiletemp = skippedjobfile.replace(".docs", ".temp")
+                                                os.rename(controllerpath + "/jobs/" + skippedjobfiletemp, controllerpath + "/jobs/reloaded/" + skippedjobfiletemp)
                                             except:
-                                                pass;
+                                                pass
                                             try:
-                                                skippedjobfileout=skippedjobfile.replace(".docs",".out");
-                                                os.rename(controllerpath+"/jobs/"+skippedjobfileout,controllerpath+"/jobs/reloaded/"+skippedjobfileout);
+                                                skippedjobfileout = skippedjobfile.replace(".docs", ".out")
+                                                os.rename(controllerpath + "/jobs/" + skippedjobfileout, controllerpath + "/jobs/reloaded/" + skippedjobfileout)
                                             except:
-                                                pass;
-                                            if skippedjobnums.count(skippedjobnum)==0:
-                                                for file in glob.iglob(controllerpath+"/jobs/*_job_"+skippedjobnum+"_*"):
-                                                    os.rename(file,file.replace("/jobs/","/jobs/reloaded/"));
-                                                for file in glob.iglob(controllerpath+"/jobs/*_job_"+skippedjobnum+".*"):
+                                                pass
+                                            if skippedjobnums.count(skippedjobnum) == 0:
+                                                for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + "_*"):
+                                                    os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
+                                                for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + ".*"):
                                                     if not ((".merge." in file) and (".out" in file)):
-                                                        os.rename(file,file.replace("/jobs/","/jobs/reloaded/"));
-                                            with open(controllerpath+"/skipped","r") as skippedstream, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream2:
-                                                skippedheader=skippedstream.readline();
-                                                tempstream2.write(skippedheader);
-                                                tempstream2.flush();
+                                                        os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
+                                            with open(controllerpath + "/skipped", "r") as skippedstream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream2:
+                                                skippedheader = skippedstream.readline()
+                                                tempstream2.write(skippedheader)
+                                                tempstream2.flush()
                                                 for skippedline in skippedstream:
                                                     if not skippedjobfile in skippedline:
-                                                        tempstream2.write(skippedline);
-                                                        tempstream2.flush();
-                                                os.rename(tempstream2.name,skippedstream.name);
-                                            i-=1;
-                                        skipped=True;
-                                    i+=1;
+                                                        tempstream2.write(skippedline)
+                                                        tempstream2.flush()
+                                                os.rename(tempstream2.name, skippedstream.name)
+                                            i -= 1
+                                        skipped = True
+                                    i += 1
                                 if not skipped:
-                                    tempstream1.write(querystateline);
+                                    tempstream1.write(querystateline)
                                     tempstream1.flush();    
-                            os.rename(tempstream1.name,querystatefilestream.name);
+                            os.rename(tempstream1.name, querystatefilestream.name)
                     else:
-                    #if querystatetierfilename!=querystatefilename+basecollection:
-                        with open(controllerpath+"/"+querystatetierfilename,"r") as querystatefilestream, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream1:
+                    #if querystatetierfilename != querystatefilename + basecollection:
+                        with open(controllerpath + "/" + querystatetierfilename, "r") as querystatefilestream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream1:
                             for querystateline in querystatefilestream:
-                                querystatelinestrip=querystateline.rstrip("\n");
-                                skipped=False;
-                                i=0;
-                                while i<len(skippedjobdocs):
-                                    if any([querystatelinestrip+"_" in x for x in skippedjobdocs[i]]):
+                                querystatelinestrip = querystateline.rstrip("\n")
+                                skipped = False
+                                i = 0
+                                while i < len(skippedjobdocs):
+                                    if any([querystatelinestrip + "_" in x for x in skippedjobdocs[i]]):
                                         for x in skippedjobdocs[i]:
-                                            if querystatelinestrip+"_" in x:
-                                                skippedjobdocs[i].remove(x);
-                                        if len(skippedjobdocs[i])==0:
-                                            skippedjobfile=skippedjobfiles[i];
-                                            skippedjobnum=skippedjobnums[i];
-                                            if not os.path.isdir(controllerpath+"/jobs/reloaded"):
-                                                os.mkdir(controllerpath+"/jobs/reloaded");
-                                            os.rename(controllerpath+"/jobs/"+skippedjobfile,controllerpath+"/jobs/reloaded/"+skippedjobfile);
-                                            del skippedjobdocs[i];
-                                            del skippedjobfiles[i];
-                                            del skippedjobnums[i];
+                                            if querystatelinestrip + "_" in x:
+                                                skippedjobdocs[i].remove(x)
+                                        if len(skippedjobdocs[i]) == 0:
+                                            skippedjobfile = skippedjobfiles[i]
+                                            skippedjobnum = skippedjobnums[i]
+                                            if not os.path.isdir(controllerpath + "/jobs/reloaded"):
+                                                os.mkdir(controllerpath + "/jobs/reloaded")
+                                            os.rename(controllerpath + "/jobs/" + skippedjobfile, controllerpath + "/jobs/reloaded/" + skippedjobfile)
+                                            del skippedjobdocs[i]
+                                            del skippedjobfiles[i]
+                                            del skippedjobnums[i]
                                             try:
-                                                skippedjobfilein=skippedjobfile.replace(".docs",".in");
-                                                os.rename(controllerpath+"/jobs/"+skippedjobfilein,controllerpath+"/jobs/reloaded/"+skippedjobfilein);
+                                                skippedjobfilein = skippedjobfile.replace(".docs", ".in")
+                                                os.rename(controllerpath + "/jobs/" + skippedjobfilein, controllerpath + "/jobs/reloaded/" + skippedjobfilein)
                                             except:
-                                                pass;
+                                                pass
                                             try:
-                                                skippedjobfiletemp=skippedjobfile.replace(".docs",".temp");
-                                                os.rename(controllerpath+"/jobs/"+skippedjobfiletemp,controllerpath+"/jobs/reloaded/"+skippedjobfiletemp);
+                                                skippedjobfiletemp = skippedjobfile.replace(".docs", ".temp")
+                                                os.rename(controllerpath + "/jobs/" + skippedjobfiletemp, controllerpath + "/jobs/reloaded/" + skippedjobfiletemp)
                                             except:
-                                                pass;
+                                                pass
                                             try:
-                                                skippedjobfileout=skippedjobfile.replace(".docs",".out");
-                                                os.rename(controllerpath+"/jobs/"+skippedjobfileout,controllerpath+"/jobs/reloaded/"+skippedjobfileout);
+                                                skippedjobfileout = skippedjobfile.replace(".docs", ".out")
+                                                os.rename(controllerpath + "/jobs/" + skippedjobfileout, controllerpath + "/jobs/reloaded/" + skippedjobfileout)
                                             except:
-                                                pass;
-                                            if skippedjobnums.count(skippedjobnum)==0:
-                                                for file in glob.iglob(controllerpath+"/jobs/*_job_"+skippedjobnum+"_*"):
-                                                    os.rename(file,file.replace("/jobs/","/jobs/reloaded/"));
-                                                for file in glob.iglob(controllerpath+"/jobs/*_job_"+skippedjobnum+".*"):
+                                                pass
+                                            if skippedjobnums.count(skippedjobnum) == 0:
+                                                for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + "_*"):
+                                                    os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
+                                                for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + ".*"):
                                                     if not ((".merge." in file) and (".out" in file)):
-                                                        os.rename(file,file.replace("/jobs/","/jobs/reloaded/"));
-                                            with open(controllerpath+"/skipped","r") as skippedstream, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream2:
-                                                skippedheader=skippedstream.readline();
-                                                tempstream2.write(skippedheader);
-                                                tempstream2.flush();
+                                                        os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
+                                            with open(controllerpath + "/skipped", "r") as skippedstream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream2:
+                                                skippedheader = skippedstream.readline()
+                                                tempstream2.write(skippedheader)
+                                                tempstream2.flush()
                                                 for skippedline in skippedstream:
                                                     if not skippedjobfile in skippedline:
-                                                        tempstream2.write(skippedline);
-                                                        tempstream2.flush();
-                                                os.rename(tempstream2.name,skippedstream.name);
-                                            i-=1;
-                                        skipped=True;
-                                    i+=1;
+                                                        tempstream2.write(skippedline)
+                                                        tempstream2.flush()
+                                                os.rename(tempstream2.name, skippedstream.name)
+                                            i -= 1
+                                        skipped = True
+                                    i += 1
                                 if not skipped:
-                                    tempstream1.write(querystateline);
-                                    tempstream1.flush();
-                            os.rename(tempstream1.name,querystatefilestream.name);
+                                    tempstream1.write(querystateline)
+                                    tempstream1.flush()
+                            os.rename(tempstream1.name, querystatefilestream.name)
                 except IOError:
-                    print "File path \""+controllerpath+"/"+querystatetierfilename+"\" does not exist.";
-                    sys.stdout.flush();
-            counters[1]-=skippeddoccount;
-            docounterupdate(counters,counterstatefile,counterheader);
+                    print "File path \"" + controllerpath + "/" + querystatetierfilename + "\" does not exist."
+                    sys.stdout.flush()
+            counters[1] -= skippeddoccount
+            docounterupdate(counters, counterstatefile, counterheader)
     except IOError:
-        print "File path \""+controllerpath+"/skipped\" does not exist.";
-        sys.stdout.flush();
+        print "File path \"" + controllerpath + "/skipped\" does not exist."
+        sys.stdout.flush()
 
-def requeueskippedreloadjobs(modname,controllername,controllerpath,reloadstatefilename,reloadpath,counters,counterstatefile,counterheader,dbindexes):
+def requeueskippedreloadjobs(modname, controllername, controllerpath, reloadstatefilename, reloadpath, counters, counterstatefile, counterheader, dbindexes):
     try:
-        skippeddoccount=0;
-        skippedjobfiles=[];
-        skippedjobnums=[];
-        skippeddocs=[];
-        skippedreloadfiles=[];
-        with open(controllerpath+"/skipped","r") as skippedstream:#, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream:
-            skippedheader=skippedstream.readline();
-            #tempstream.write(skippedheader);
-            #tempstream.flush();
+        skippeddoccount = 0
+        skippedjobfiles = []
+        skippedjobnums = []
+        skippeddocs = []
+        skippedreloadfiles = []
+        with open(controllerpath + "/skipped", "r") as skippedstream:#, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream:
+            skippedheader = skippedstream.readline()
+            #tempstream.write(skippedheader)
+            #tempstream.flush()
             for line in skippedstream:
-                skippedjobfile,exitcode,resubmitq=line.rstrip("\n").split(",");
+                skippedjobfile, exitcode, resubmitq = line.rstrip("\n").split(",")
                 if eval(resubmitq):
-                    skippedjobfilesplit=skippedjobfile.split("_");
-                    if skippedjobfilesplit[:2]==[modname,controllername]:
-                        #if niters==1:
-                        #elif niters>1:
-                        with open(controllerpath+"/jobs/"+skippedjobfile,"r") as skippeddocstream:
-                            #skippeddocsline=skippeddocstream.readline();
+                    skippedjobfilesplit = skippedjobfile.split("_")
+                    if skippedjobfilesplit[:2] == [modname, controllername]:
+                        #if niters == 1:
+                        #elif niters > 1:
+                        with open(controllerpath + "/jobs/" + skippedjobfile, "r") as skippeddocstream:
+                            #skippeddocsline = skippeddocstream.readline()
                             for skippeddocsline in skippeddocstream:
                                 if "@" in skippeddocsline:
-                                    #print skippeddocsline;
-                                    #sys.stdout.flush();
-                                    skippedinfoline=("{"+skippeddocsline.rstrip("\n").split(".{")[1]).split("<");
-                                    #print skippedinfoline;
-                                    #sys.stdout.flush();
-                                    skippedjobfiles+=[skippedjobfile];
-                                    skippedjobnums+=[re.sub(".*_job_([0-9]+)[_\.].*",r"\1",skippedjobfile)];
-                                    skippeddocs+=["_".join(indexdoc2indexsplit(json.loads(skippedinfoline[0]),dbindexes))];
-                                    skippedreloadfiles+=[skippedinfoline[1]];
-                                    skippeddoccount+=1;
-                                    #if skippedfileposdoc[reloadstatefilename+"FILE"] in reloadfiles.keys():
-                                    #    reloadpos+=[skippedfileposdoc[reloadstatefilename+"POS"]];
-                                    #skippeddoc=json.loads(skippedjobids);
-                                    #skippedjobfiles+=[skippedjobfile];
-                                    #skippedjobpos+=["_".join(indexdoc2indexsplit(skippeddoc,dbindexes))];
-                                    ##print(skippeddoc);
-                                    ##sys.stdout.flush();
-                                    #skippeddocsline=skippeddocstream.readline();
-                        #os.remove(controllerpath+"/jobs/"+skippedjob+".docs");
-                        #os.remove(controllerpath+"/jobs/"+skippedjob+".docs.in");
-                        #os.remove(controllerpath+"/jobs/"+skippedjob+".error");
-                        #os.remove(controllerpath+"/jobs/"+skippedjob+".batch.log");
+                                    #print skippeddocsline
+                                    #sys.stdout.flush()
+                                    skippedinfoline = ("{" + skippeddocsline.rstrip("\n").split(".{")[1]).split("<")
+                                    #print skippedinfoline
+                                    #sys.stdout.flush()
+                                    skippedjobfiles += [skippedjobfile]
+                                    skippedjobnums += [re.sub(".*_job_([0-9] + )[_\.].*", r"\1", skippedjobfile)]
+                                    skippeddocs += ["_".join(indexdoc2indexsplit(json.loads(skippedinfoline[0]), dbindexes))]
+                                    skippedreloadfiles += [skippedinfoline[1]]
+                                    skippeddoccount += 1
+                                    #if skippedfileposdoc[reloadstatefilename + "FILE"] in reloadfiles.keys():
+                                    #    reloadpos += [skippedfileposdoc[reloadstatefilename + "POS"]]
+                                    #skippeddoc = json.loads(skippedjobids)
+                                    #skippedjobfiles += [skippedjobfile]
+                                    #skippedjobpos += ["_".join(indexdoc2indexsplit(skippeddoc, dbindexes))]
+                                    ##print(skippeddoc)
+                                    ##sys.stdout.flush()
+                                    #skippeddocsline = skippeddocstream.readline()
+                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".docs")
+                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".docs.in")
+                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".error")
+                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".batch.log")
                     #else:
-                    #    tempstream.write(line);
-                    #    tempstream.flush();
+                    #    tempstream.write(line)
+                    #    tempstream.flush()
                 #else:
-                #    tempstream.write(line);
-                #    tempstream.flush();
-            #os.rename(tempstream.name,skippedstream.name);
-            #if len(skippeddocs)>0:
-            #    reloadfilescurs=glob.iglob(controllerpath+"/jobs/"+reloadpattern);
-            #    skippedjobfilescollect=[];
-            #    skippedjobposcollect=[];
+                #    tempstream.write(line)
+                #    tempstream.flush()
+            #os.rename(tempstream.name, skippedstream.name)
+            #if len(skippeddocs) > 0:
+            #    reloadfilescurs = glob.iglob(controllerpath + "/jobs/" + reloadpattern)
+            #    skippedjobfilescollect = []
+            #    skippedjobposcollect = []
             #    for reloadfile in reloadfilescurs:
-            #        #print(reloadfile);
-            #        #sys.stdout.flush();
-            #        with open(reloadfile,"r") as reloaddocstream:
-            #            reloaddocsline=reloaddocstream.readline();
-            #            while (reloaddocsline!="") and (len(skippedjobpos)>0):
-            #                while (reloaddocsline!="") and ("@" not in reloaddocsline):
-            #                    reloaddocsline=reloaddocstream.readline();
+            #        #print(reloadfile)
+            #        #sys.stdout.flush()
+            #        with open(reloadfile, "r") as reloaddocstream:
+            #            reloaddocsline = reloaddocstream.readline()
+            #            while (reloaddocsline != "") and (len(skippedjobpos) > 0):
+            #                while (reloaddocsline != "") and ("@" not in reloaddocsline):
+            #                    reloaddocsline = reloaddocstream.readline()
             #                if "@" in reloaddocsline:
-            #                    reloaddoc=json.loads("{"+reloaddocsline.rstrip("\n").split(".{")[1]);
-            #                    reloadid="_".join(indexdoc2indexsplit(reloaddoc,dbindexes));
+            #                    reloaddoc = json.loads("{" + reloaddocsline.rstrip("\n").split(".{")[1])
+            #                    reloadid = "_".join(indexdoc2indexsplit(reloaddoc, dbindexes))
             #                    if reloadid in skippedjobpos:
-            #                        skippedjobindex=skippedjobpos.index(reloadid);
-            #                        reloadfiles+=[reloadfile.split("/")[-1]];
-            #                        reloadids+=[reloadid];
-            #                        skippeddoccount+=1;
-            #                        #skippedjobdocs.remove(reloaddocsline);
-            #                        skippedjobfilescollect+=[skippedjobfiles[skippedjobindex]];
-            #                        skippedjobposcollect+=[skippedjobpos[skippedjobindex]];
-            #                        del skippedjobfiles[skippedjobindex];
-            #                        del skippedjobpos[skippedjobindex];
-            #                    reloaddocsline=reloaddocstream.readline();
-            #        if len(skippedjobpos)==0:
-            #            break;
-            #        #if len(reloadfilesplits)>0:
-            #        #    reloadids+=[reloadfilesplits];
-            #        #    reloadfiles+=[reloadfile.split("/")[-1]];
-            #    skippedjobfiles+=skippedjobfilescollect;
-            #    skippedjobpos+=skippedjobposcollect;
-            #os.rename(tempstream.name,skippedstream.name);
-        if len(skippeddocs)>0:
+            #                        skippedjobindex = skippedjobpos.index(reloadid)
+            #                        reloadfiles += [reloadfile.split("/")[-1]]
+            #                        reloadids += [reloadid]
+            #                        skippeddoccount += 1
+            #                        #skippedjobdocs.remove(reloaddocsline)
+            #                        skippedjobfilescollect += [skippedjobfiles[skippedjobindex]]
+            #                        skippedjobposcollect += [skippedjobpos[skippedjobindex]]
+            #                        del skippedjobfiles[skippedjobindex]
+            #                        del skippedjobpos[skippedjobindex]
+            #                    reloaddocsline = reloaddocstream.readline()
+            #        if len(skippedjobpos) == 0:
+            #            break
+            #        #if len(reloadfilesplits) > 0:
+            #        #    reloadids += [reloadfilesplits]
+            #        #    reloadfiles += [reloadfile.split("/")[-1]]
+            #    skippedjobfiles += skippedjobfilescollect
+            #    skippedjobpos += skippedjobposcollect
+            #os.rename(tempstream.name, skippedstream.name)
+        if len(skippeddocs) > 0:
             try:
-                with open(controllerpath+"/"+reloadstatefilename+"FILE","r") as reloadstatefilestream, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream1:
+                with open(controllerpath + "/" + reloadstatefilename + "FILE", "r") as reloadstatefilestream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream1:
                     for reloadstateline in reloadstatefilestream:
-                        reloadstatelinefile=reloadstateline.rstrip("\n");
+                        reloadstatelinefile = reloadstateline.rstrip("\n")
                         if reloadstatelinefile in skippedreloadfiles:
-                            with open(controllerpath+"/"+reloadpath+"/"+reloadstatelinefile,"r") as reloaddocsstream, open(controllerpath+"/"+reloadstatefilename+"DOC","a") as reloadstatedocsstream:
+                            with open(controllerpath + "/" + reloadpath + "/" + reloadstatelinefile, "r") as reloaddocsstream, open(controllerpath + "/" + reloadstatefilename + "DOC", "a") as reloadstatedocsstream:
                                 for reloaddocsline in reloaddocsstream:
                                     if "@" in reloaddocsline:
-                                        reloaddoc=json.loads("{"+reloaddocsline.rstrip("\n").split(".{")[1]);
-                                        reloaddocform="_".join(indexdoc2indexsplit(reloaddoc,dbindexes));
+                                        reloaddoc = json.loads("{" + reloaddocsline.rstrip("\n").split(".{")[1])
+                                        reloaddocform = "_".join(indexdoc2indexsplit(reloaddoc, dbindexes))
                                         if reloaddocform not in skippeddocs:
-                                            reloadstatedocsstream.write(reloadstatelinefile+"~"+reloaddocform+"\n");
-                                            reloadstatedocsstream.flush();
+                                            reloadstatedocsstream.write(reloadstatelinefile + "~" + reloaddocform + "\n")
+                                            reloadstatedocsstream.flush()
                                         else:
-                                            skippeddocindex=skippeddocs.index(reloaddocform);
-                                            skippedjobfile=skippedjobfiles[skippeddocindex];
-                                            skippedjobnum=skippedjobnums[skippeddocindex];
-                                            del skippeddocs[skippeddocindex];
-                                            del skippedjobfiles[skippeddocindex];
-                                            del skippedjobnums[skippeddocindex];
-                                            del skippedreloadfiles[skippeddocindex];
-                                            if skippedjobfiles.count(skippedjobfile)==0:
-                                                if not os.path.isdir(controllerpath+"/jobs/reloaded"):
-                                                    os.mkdir(controllerpath+"/jobs/reloaded");
-                                                os.rename(controllerpath+"/jobs/"+skippedjobfile,controllerpath+"/jobs/reloaded/"+skippedjobfile);
+                                            skippeddocindex = skippeddocs.index(reloaddocform)
+                                            skippedjobfile = skippedjobfiles[skippeddocindex]
+                                            skippedjobnum = skippedjobnums[skippeddocindex]
+                                            del skippeddocs[skippeddocindex]
+                                            del skippedjobfiles[skippeddocindex]
+                                            del skippedjobnums[skippeddocindex]
+                                            del skippedreloadfiles[skippeddocindex]
+                                            if skippedjobfiles.count(skippedjobfile) == 0:
+                                                if not os.path.isdir(controllerpath + "/jobs/reloaded"):
+                                                    os.mkdir(controllerpath + "/jobs/reloaded")
+                                                os.rename(controllerpath + "/jobs/" + skippedjobfile, controllerpath + "/jobs/reloaded/" + skippedjobfile)
                                                 try:
-                                                    skippedjobfilein=skippedjobfile.replace(".docs",".in");
-                                                    os.rename(controllerpath+"/jobs/"+skippedjobfilein,controllerpath+"/jobs/reloaded/"+skippedjobfilein);
+                                                    skippedjobfilein = skippedjobfile.replace(".docs", ".in")
+                                                    os.rename(controllerpath + "/jobs/" + skippedjobfilein, controllerpath + "/jobs/reloaded/" + skippedjobfilein)
                                                 except:
-                                                    pass;
+                                                    pass
                                                 try:
-                                                    skippedjobfiletemp=skippedjobfile.replace(".docs",".temp");
-                                                    os.rename(controllerpath+"/jobs/"+skippedjobfiletemp,controllerpath+"/jobs/reloaded/"+skippedjobfiletemp);
+                                                    skippedjobfiletemp = skippedjobfile.replace(".docs", ".temp")
+                                                    os.rename(controllerpath + "/jobs/" + skippedjobfiletemp, controllerpath + "/jobs/reloaded/" + skippedjobfiletemp)
                                                 except:
-                                                    pass;
+                                                    pass
                                                 try:
-                                                    skippedjobfileout=skippedjobfile.replace(".docs",".out");
-                                                    os.rename(controllerpath+"/jobs/"+skippedjobfileout,controllerpath+"/jobs/reloaded/"+skippedjobfileout);
+                                                    skippedjobfileout = skippedjobfile.replace(".docs", ".out")
+                                                    os.rename(controllerpath + "/jobs/" + skippedjobfileout, controllerpath + "/jobs/reloaded/" + skippedjobfileout)
                                                 except:
-                                                    pass;
-                                                if skippedjobnums.count(skippedjobnum)==0:
-                                                    for file in glob.iglob(controllerpath+"/jobs/*_job_"+skippedjobnum+"_*"):
-                                                        os.rename(file,file.replace("/jobs/","/jobs/reloaded/"));
-                                                    for file in glob.iglob(controllerpath+"/jobs/*_job_"+skippedjobnum+".*"):
+                                                    pass
+                                                if skippedjobnums.count(skippedjobnum) == 0:
+                                                    for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + "_*"):
+                                                        os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
+                                                    for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + ".*"):
                                                         if not ((".merge." in file) and (".out" in file)):
-                                                            os.rename(file,file.replace("/jobs/","/jobs/reloaded/"));
-                                                with open(controllerpath+"/skipped","r") as skippedstream, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream2:
-                                                    skippedheader=skippedstream.readline();
-                                                    tempstream2.write(skippedheader);
-                                                    tempstream2.flush();
+                                                            os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
+                                                with open(controllerpath + "/skipped", "r") as skippedstream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream2:
+                                                    skippedheader = skippedstream.readline()
+                                                    tempstream2.write(skippedheader)
+                                                    tempstream2.flush()
                                                     for skippedline in skippedstream:
                                                         if not skippedjobfile in skippedline:
-                                                            tempstream2.write(skippedline);
-                                                            tempstream2.flush();
-                                                    #print("a");
-                                                    #sys.stdout.flush();
-                                                    os.rename(tempstream2.name,skippedstream.name);
-                                                    #print("b");
-                                                    #sys.stdout.flush();
+                                                            tempstream2.write(skippedline)
+                                                            tempstream2.flush()
+                                                    #print("a")
+                                                    #sys.stdout.flush()
+                                                    os.rename(tempstream2.name, skippedstream.name)
+                                                    #print("b")
+                                                    #sys.stdout.flush()
                         else:
-                            tempstream1.write(reloadstateline);
-                            tempstream1.flush();
-                    os.rename(tempstream1.name,reloadstatefilestream.name);
+                            tempstream1.write(reloadstateline)
+                            tempstream1.flush()
+                    os.rename(tempstream1.name, reloadstatefilestream.name)
             except IOError:
-                print "File path \""+controllerpath+"/"+reloadstatefilename+"FILE"+"\" does not exist.";
-                sys.stdout.flush();
+                print "File path \"" + controllerpath + "/" + reloadstatefilename + "FILE" + "\" does not exist."
+                sys.stdout.flush()
             try:
-                with open(controllerpath+"/"+reloadstatefilename+"DOC","r") as reloadstatefilestream, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream1:
+                with open(controllerpath + "/" + reloadstatefilename + "DOC", "r") as reloadstatefilestream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream1:
                     for reloadstateline in reloadstatefilestream:
-                        reloadstatelinefile,reloadstatelinedoc=reloadstateline.rstrip("\n").split("~");
+                        reloadstatelinefile, reloadstatelinedoc = reloadstateline.rstrip("\n").split("~")
                         if reloadstatelinedoc in skippeddocs:
-                            skippedindex=skippeddocs.index(reloadstatelinedoc);
-                            if reloadstatelinefile==skippedreloadfiles[skippedindex]:
-                                skippedjobfile=skippedjobfiles[skippedindex];
-                                skippedjobnum=skippedjobnums[skippedindex];
-                                del skippeddocs[skippeddocindex];
-                                del skippedjobfiles[skippeddocindex];
-                                del skippedjobnums[skippeddocindex];
-                                del skippedreloadfiles[skippeddocindex];
-                                if skippedjobfiles.count(skippedjobfile)==0:
-                                    if not os.path.isdir(controllerpath+"/jobs/reloaded"):
-                                        os.mkdir(controllerpath+"/jobs/reloaded");
-                                    os.rename(controllerpath+"/jobs/"+skippedjobfile,controllerpath+"/jobs/reloaded/"+skippedjobfile);
+                            skippedindex = skippeddocs.index(reloadstatelinedoc)
+                            if reloadstatelinefile == skippedreloadfiles[skippedindex]:
+                                skippedjobfile = skippedjobfiles[skippedindex]
+                                skippedjobnum = skippedjobnums[skippedindex]
+                                del skippeddocs[skippeddocindex]
+                                del skippedjobfiles[skippeddocindex]
+                                del skippedjobnums[skippeddocindex]
+                                del skippedreloadfiles[skippeddocindex]
+                                if skippedjobfiles.count(skippedjobfile) == 0:
+                                    if not os.path.isdir(controllerpath + "/jobs/reloaded"):
+                                        os.mkdir(controllerpath + "/jobs/reloaded")
+                                    os.rename(controllerpath + "/jobs/" + skippedjobfile, controllerpath + "/jobs/reloaded/" + skippedjobfile)
                                     try:
-                                        skippedjobfilein=skippedjobfile.replace(".docs",".in");
-                                        os.rename(controllerpath+"/jobs/"+skippedjobfilein,controllerpath+"/jobs/reloaded/"+skippedjobfilein);
+                                        skippedjobfilein = skippedjobfile.replace(".docs", ".in")
+                                        os.rename(controllerpath + "/jobs/" + skippedjobfilein, controllerpath + "/jobs/reloaded/" + skippedjobfilein)
                                     except:
-                                        pass;
+                                        pass
                                     try:
-                                        skippedjobfiletemp=skippedjobfile.replace(".docs",".temp");
-                                        os.rename(controllerpath+"/jobs/"+skippedjobfiletemp,controllerpath+"/jobs/reloaded/"+skippedjobfiletemp);
+                                        skippedjobfiletemp = skippedjobfile.replace(".docs", ".temp")
+                                        os.rename(controllerpath + "/jobs/" + skippedjobfiletemp, controllerpath + "/jobs/reloaded/" + skippedjobfiletemp)
                                     except:
-                                        pass;
+                                        pass
                                     try:
-                                        skippedjobfileout=skippedjobfile.replace(".docs",".out");
-                                        os.rename(controllerpath+"/jobs/"+skippedjobfileout,controllerpath+"/jobs/reloaded/"+skippedjobfileout);
+                                        skippedjobfileout = skippedjobfile.replace(".docs", ".out")
+                                        os.rename(controllerpath + "/jobs/" + skippedjobfileout, controllerpath + "/jobs/reloaded/" + skippedjobfileout)
                                     except:
-                                        pass;
-                                    if skippedjobnums.count(skippedjobnum)==0:
-                                        for file in glob.iglob(controllerpath+"/jobs/*_job_"+skippedjobnum+"_*"):
-                                            os.rename(file,file.replace("/jobs/","/jobs/reloaded/"));
-                                        for file in glob.iglob(controllerpath+"/jobs/*_job_"+skippedjobnum+".*"):
+                                        pass
+                                    if skippedjobnums.count(skippedjobnum) == 0:
+                                        for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + "_*"):
+                                            os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
+                                        for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + ".*"):
                                             if not ((".merge." in file) and (".out" in file)):
-                                                os.rename(file,file.replace("/jobs/","/jobs/reloaded/"));
-                                    with open(controllerpath+"/skipped","r") as skippedstream, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream2:
-                                        skippedheader=skippedstream.readline();
-                                        tempstream2.write(skippedheader);
-                                        tempstream2.flush();
+                                                os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
+                                    with open(controllerpath + "/skipped", "r") as skippedstream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream2:
+                                        skippedheader = skippedstream.readline()
+                                        tempstream2.write(skippedheader)
+                                        tempstream2.flush()
                                         for skippedline in skippedstream:
                                             if not skippedjobfile in skippedline:
-                                                tempstream2.write(skippedline);
-                                                tempstream2.flush();
-                                        #print("c");
-                                        #sys.stdout.flush();
-                                        os.rename(tempstream2.name,skippedstream.name);
-                                        #print("d");
-                                        #sys.stdout.flush();
+                                                tempstream2.write(skippedline)
+                                                tempstream2.flush()
+                                        #print("c")
+                                        #sys.stdout.flush()
+                                        os.rename(tempstream2.name, skippedstream.name)
+                                        #print("d")
+                                        #sys.stdout.flush()
                         else:
-                            tempstream1.write(reloadstateline);
-                            tempstream1.flush();
-                    os.rename(tempstream1.name,reloadstatefilestream.name);
+                            tempstream1.write(reloadstateline)
+                            tempstream1.flush()
+                    os.rename(tempstream1.name, reloadstatefilestream.name)
             except IOError:
-                print "File path \""+controllerpath+"/"+reloadstatefilename+"DOC"+"\" does not exist.";
-                sys.stdout.flush();
-            counters[1]-=skippeddoccount;
-            docounterupdate(counters,counterstatefile,counterheader);
+                print "File path \"" + controllerpath + "/" + reloadstatefilename + "DOC" + "\" does not exist."
+                sys.stdout.flush()
+            counters[1] -= skippeddoccount
+            docounterupdate(counters, counterstatefile, counterheader)
     except IOError:
-        print "File path \""+controllerpath+"/skipped\" does not exist.";
-        sys.stdout.flush();
+        print "File path \"" + controllerpath + "/skipped\" does not exist."
+        sys.stdout.flush()
 
 def orderpartitions(partitions):
-    partitionsidle=get_partitionsidle(partitions);
-    #partsmix=Popen("sinfo -o '%t %P' | grep 'ser-par-10g' | grep 'mix' | cut -d' ' -f2 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
-    #partsalloc=Popen("sinfo -o '%t %P' | grep 'ser-par-10g' | grep 'alloc' | cut -d' ' -f2 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
-    #partslist=','.join(partsidle+[x for x in partsmix if x not in partsidle]+[x for x in partsalloc if x not in partsidle+partsmix]);
-    partitionscomp=get_partitionscomp(partitions);
-    partitionsrun=get_partitionsrun(partitions);
-    partitionspend=get_partitionspend(partitions);
-    #print "orderpartitions";
-    #print "for rawpart in $(sinfo -h -o '%t %c %D %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'idle' | cut -d' ' -f2,3,4 | sed 's/\*//g' | sed 's/\s/,/g'); do echo $(echo $rawpart | cut -d',' -f1,2 | sed 's/,/*/g' | bc),$(echo $rawpart | cut -d',' -f3); done | tr ' ' '\n' | sort -t',' -k1 -n | cut -d',' -f2 | tr '\n' ',' | head -c -1";
-    #print "squeue -h -o '%P %T %L' | grep -E '("+greppartitions+")\*?\s*$' | grep 'COMPLETING' | sort -k2,2 -k3,3n | cut -d' ' -f1 | tr '\n' ',' | head -c -1";
-    #print "squeue -h -o '%P %T %L' | grep -E '("+greppartitions+")\*?\s*$' | grep 'RUNNING' | sort -k2,2 -k3,3n | cut -d' ' -f1 | tr '\n' ',' | head -c -1";
-    #print "squeue -h -o '%P %T %L' | grep -E '("+greppartitions+")\*?\s*$' | grep 'PENDING' | sort -k2,2 -k3,3n | cut -d' ' -f1 | tr '\n' ',' | head -c -1";
-    #print "";
-    #sys.stdout.flush();
-    orderedpartitions=[x for x in deldup(partitionsidle+partitionscomp+partitionsrun+partitionspend+partitions) if x!=""];
-    return orderedpartitions;
+    partitionsidle = get_partitionsidle(partitions)
+    #partsmix = Popen("sinfo -o '%t %P' | grep 'ser-par-10g' | grep 'mix' | cut -d' ' -f2 | sed 's/\*//g' | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(',')
+    #partsalloc = Popen("sinfo -o '%t %P' | grep 'ser-par-10g' | grep 'alloc' | cut -d' ' -f2 | sed 's/\*//g' | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(',')
+    #partslist = ', '.join(partsidle + [x for x in partsmix if x not in partsidle] + [x for x in partsalloc if x not in partsidle + partsmix])
+    partitionscomp = get_partitionscomp(partitions)
+    partitionsrun = get_partitionsrun(partitions)
+    partitionspend = get_partitionspend(partitions)
+    #print "orderpartitions"
+    #print "for rawpart in $(sinfo -h -o '%t %c %D %P' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'idle' | cut -d' ' -f2, 3,4 | sed 's/\*//g' | sed 's/\s/, /g'); do echo $(echo $rawpart | cut -d', ' -f1, 2 | sed 's/, /*/g' | bc), $(echo $rawpart | cut -d', ' -f3); done | tr ' ' '\n' | sort -t', ' -k1 -n | cut -d', ' -f2 | tr '\n' ', ' | head -c -1"
+    #print "squeue -h -o '%P %T %L' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'COMPLETING' | sort -k2, 2 -k3, 3n | cut -d' ' -f1 | tr '\n' ', ' | head -c -1"
+    #print "squeue -h -o '%P %T %L' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'RUNNING' | sort -k2, 2 -k3, 3n | cut -d' ' -f1 | tr '\n' ', ' | head -c -1"
+    #print "squeue -h -o '%P %T %L' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'PENDING' | sort -k2, 2 -k3, 3n | cut -d' ' -f1 | tr '\n' ', ' | head -c -1"
+    #print ""
+    #sys.stdout.flush()
+    orderedpartitions = [x for x in deldup(partitionsidle + partitionscomp + partitionsrun + partitionspend + partitions) if x != ""]
+    return orderedpartitions
 
 def orderfreepartitions(partitions):
-    partitionsidle=get_partitionsidle(partitions);
-    partitionscomp=get_partitionscomp(partitions);
-    orderedfreepartitions=[x for x in deldup(partitionsidle+partitionscomp) if x!=""];
-    return orderedfreepartitions;
+    partitionsidle = get_partitionsidle(partitions)
+    partitionscomp = get_partitionscomp(partitions)
+    orderedfreepartitions = [x for x in deldup(partitionsidle + partitionscomp) if x != ""]
+    return orderedfreepartitions
 
-#def getmaxmemorypernode(resourcesstatefile,partition):
-#    maxmemorypernode=0;
+#def getmaxmemorypernode(resourcesstatefile, partition):
+#    maxmemorypernode = 0
 #    try:
-#        with open(resourcesstatefile,"r") as resourcesstream:
-#            resourcesheader=resourcesstream.readline();
+#        with open(resourcesstatefile, "r") as resourcesstream:
+#            resourcesheader = resourcesstream.readline()
 #            for resourcesstring in resourcesstream:
-#                resources=resourcesstring.rstrip("\n").split(",");
-#                if resources[0]==partition:
-#                    maxmemorypernode=eval(resources[1]);
-#                    break;
+#                resources = resourcesstring.rstrip("\n").split(",")
+#                if resources[0] == partition:
+#                    maxmemorypernode = eval(resources[1])
+#                    break
 #    except IOError:
-#        print "File path \""+resourcesstatefile+"\" does not exist.";
-#        sys.stdout.flush();
-#    return maxmemorypernode;
-
-def distributeovernodes(partition,maxmemorypernode,scriptmemorylimit,nnodes,localmaxstepcount,niters):#,maxthreads):
-    #print partitions;
-    #sys.stdout.flush();
-    #partition=partitions[0];
-    partitionncorespernode=get_partitionncorespernode(partition);
-    ncores=nnodes*partitionncorespernode;
-    maxnnodes=get_maxnnodes(partition);
-    #print "distributeovernodes";
-    #print "sinfo -h -p '"+partition+"' -o '%c' | head -n1 | head -c -1";
-    #print "scontrol show partition '"+partition+"' | grep 'MaxNodes=' | sed 's/^.*\sMaxNodes=\([0-9]*\)\s.*$/\\1/g' | head -c -1";
-    #print "";
-    #sys.stdout.flush();
-    #maxmemorypernode=getmaxmemorypernode(resourcesstatefile,partition);
-    #tempnnodes=nnodes;
-    #if scriptmemorylimit=="":
-    #    nstepsdistribmempernode=float(maxsteps/tempnnodes);
-    #    while (tempnnodes>1) and (nstepsdistribmempernode<1):
-    #        tempnnodes-=1;
-    #        nstepsdistribmempernode=float(maxsteps/tempnnodes);
+#        print "File path \"" + resourcesstatefile + "\" does not exist."
+#        sys.stdout.flush()
+#    return maxmemorypernode
+
+def distributeovernodes(partition, maxmemorypernode, scriptmemorylimit, nnodes, localmaxstepcount, niters):#, maxthreads):
+    #print partitions
+    #sys.stdout.flush()
+    #partition = partitions[0]
+    partitionncorespernode = get_partitionncorespernode(partition)
+    ncores = nnodes * partitionncorespernode
+    maxnnodes = get_maxnnodes(partition)
+    #print "distributeovernodes"
+    #print "sinfo -h -p '" + partition + "' -o '%c' | head -n1 | head -c -1"
+    #print "scontrol show partition '" + partition + "' | grep 'MaxNodes = ' | sed 's/^.*\sMaxNodes = \([0-9]*\)\s.*$/\\1/g' | head -c -1"
+    #print ""
+    #sys.stdout.flush()
+    #maxmemorypernode = getmaxmemorypernode(resourcesstatefile, partition)
+    #tempnnodes = nnodes
+    #if scriptmemorylimit == "":
+    #    nstepsdistribmempernode = float(maxsteps / tempnnodes)
+    #    while (tempnnodes > 1) and (nstepsdistribmempernode < 1):
+    #        tempnnodes -= 1
+    #        nstepsdistribmempernode = float(maxsteps / tempnnodes)
     #else:
-    if (scriptmemorylimit=="") or (eval(scriptmemorylimit)==maxmemorypernode):
-        nsteps=min(nnodes,localmaxstepcount)*niters;
-        #memoryperstep=maxmemorypernode;
-    elif eval(scriptmemorylimit)>maxmemorypernode:
-        return None;
+    if (scriptmemorylimit == "") or (eval(scriptmemorylimit) == maxmemorypernode):
+        nsteps = min(nnodes, localmaxstepcount) * niters
+        #memoryperstep = maxmemorypernode
+    elif eval(scriptmemorylimit) > maxmemorypernode:
+        return None
     else:
-        nstepsdistribmem=nnodes*maxmemorypernode/eval(scriptmemorylimit);
-        #print "a: "+str(nstepsdistribmem);
-        nsteps=min(ncores,localmaxstepcount,nstepsdistribmem)*niters;
-        #print "b: "+str(nsteps);
-        #memoryperstep=nnodes*maxmemorypernode/nsteps;
-        #print "c: "+str(memoryperstep);
-
-    #nstepsfloat=min(float(ndocsleft),float(partitionncorespernode),nstepsdistribmempernode);
-    #nnodes=int(min(maxnnodes,math.ceil(1./nstepsfloat)));
-    #ncores=nnodes*partitionncorespernode;
-    #nsteps=int(max(1,nstepsfloat));
-    #nnodes=1;
-    #if nstepsdistribmempernode<1:
-    #    if len(partitions)>1:
-    #        return distributeovernodes(resourcesstatefile,partitions[1:],scriptmemorylimit,nnodes,maxsteps,maxthreads);
+        nstepsdistribmem = nnodes * maxmemorypernode / eval(scriptmemorylimit)
+        #print "a: " + str(nstepsdistribmem)
+        nsteps = min(ncores, localmaxstepcount, nstepsdistribmem) * niters
+        #print "b: " + str(nsteps)
+        #memoryperstep = nnodes * maxmemorypernode / nsteps
+        #print "c: " + str(memoryperstep)
+
+    #nstepsfloat = min(float(ndocsleft), float(partitionncorespernode), nstepsdistribmempernode)
+    #nnodes = int(min(maxnnodes, math.ceil(1./nstepsfloat)))
+    #ncores = nnodes * partitionncorespernode
+    #nsteps = int(max(1, nstepsfloat))
+    #nnodes = 1
+    #if nstepsdistribmempernode < 1:
+    #    if len(partitions) > 1:
+    #        return distributeovernodes(resourcesstatefile, partitions[1:], scriptmemorylimit, nnodes, maxsteps, maxthreads)
     #    else:
-    #        print "Memory requirement is too large for this cluster.";
-    #        sys.stdout.flush();
-    #nnodes=tempnnodes;
-    #nsteps=min(ncores,nstepsdistribmem,maxsteps);
-    #nsteps=int(nstepsfloat);
-    #if nsteps>0:
-    #    memoryperstep=nnodes*maxmemorypernode/nsteps;
+    #        print "Memory requirement is too large for this cluster."
+    #        sys.stdout.flush()
+    #nnodes = tempnnodes
+    #nsteps = min(ncores, nstepsdistribmem, maxsteps)
+    #nsteps = int(nstepsfloat)
+    #if nsteps > 0:
+    #    memoryperstep = nnodes * maxmemorypernode / nsteps
     #else:
-    #    memoryperstep=maxmemorypernode;
-    return [ncores,nsteps,maxmemorypernode];
-
-def writejobfile(reloadjob,modname,logging,cleanup,templocal,writelocal,writedb,statslocal,statsdb,markdone,jobname,jobstepnames,controllerpath,controllername,writemode,partitiontimelimit,buffertimelimit,partition,nnodes,counters,totmem,ncoresused,scriptlanguage,scriptcommand,scriptflags,scriptext,scriptargs,base,dbindexes,nthreadsfield,nbatch,nworkers,docbatches):
-    ndocbatches=len(docbatches);
-    outputlinemarkers=["-","+","&","@","CPUTime:","MaxRSS:","MaxVMSize:","BSONSize:","None"];
-    jobstring="#!/bin/bash\n";
-    jobstring+="\n";
-    jobstring+="#Created "+str(datetime.datetime.now().strftime("%Y %m %d %H:%M:%S"))+"\n";
-    jobstring+="\n";
-    jobstring+="#Job name\n";
-    jobstring+="#SBATCH -J \""+jobname+"\"\n";
-    jobstring+="#################\n";
-    jobstring+="#Working directory\n";
-    jobstring+="#SBATCH -D \""+controllerpath+"/jobs\"\n";
-    jobstring+="#################\n";
-    jobstring+="#Job output file\n";
-    jobstring+="#SBATCH -o \""+jobname+".log\"\n";
-    jobstring+="#################\n";
-    jobstring+="#Job error file\n";
-    jobstring+="#SBATCH -e \""+jobname+".err\"\n";
-    jobstring+="#################\n";
-    jobstring+="#Job file write mode\n";
-    jobstring+="#SBATCH --open-mode=\""+writemode+"\"\n";
-    jobstring+="#################\n";
-    jobstring+="#Job max time\n";
-    jobstring+="#SBATCH --time=\""+partitiontimelimit+"\"\n";
-    jobstring+="#################\n";
-    jobstring+="#Partition (queue) to use for job\n";
-    jobstring+="#SBATCH --partition=\""+partition+"\"\n";
-    jobstring+="#################\n";
-    jobstring+="#Number of tasks (CPUs) allocated for job\n";
-    jobstring+="#SBATCH -n "+str(ncoresused)+"\n";
-    jobstring+="#################\n";
-    jobstring+="#Number of nodes to distribute n tasks across\n";
-    jobstring+="#SBATCH -N "+str(nnodes)+"\n";
-    jobstring+="#################\n";
-    #jobstring+="#Lock down N nodes for job\n";
-    #jobstring+="#SBATCH --exclusive\n";
-    #jobstring+="#################\n";
-    jobstring+="\n";
-    jobstring+="#Job info\n";
-    jobstring+="modname=\""+modname+"\"\n";
-    jobstring+="controllername=\""+controllername+"\"\n";
-    #jobstring+="outputlinemarkers=\""+str(outputlinemarkers).replace(" ","")+"\"\n";
-    #jobstring+="jobnum="+str(counters[0])+"\n";
-    jobstring+="nsteps="+str(ncoresused)+"\n";
-    jobstring+="memunit=\"M\"\n";
-    jobstring+="totmem="+str(totmem/1000000)+"\n";
-    #jobstring+="stepmem=$((${totmem}/${nsteps}))\n";
-    jobstring+="steptime=\""+buffertimelimit+"\"\n";
-    jobstring+="scriptlanguage=\""+scriptlanguage+"\"\n";
-    jobstring+="nbatch="+str(nbatch)+"\n";
-    jobstring+="nworkers="+str(min(nworkers,ncoresused))+"\n";
-    jobstring+="\n";
-    jobstring+="#Option info\n";
-    #jobstring+="logging=\""+str(logging)+"\"\n";
-    #jobstring+="cleanup=\""+str(cleanup)+"\"\n";
-    #jobstring+="templocal=\""+str(templocal)+"\"\n";
-    #jobstring+="writelocal=\""+str(writelocal)+"\"\n";
-    #jobstring+="writedb=\""+str(writedb)+"\"\n";
-    #jobstring+="statslocal=\""+str(statslocal)+"\"\n";
-    #jobstring+="statsdb=\""+str(statsdb)+"\"\n";
-    jobstring+="markdone=\""+markdone+"\"\n";
-    jobstring+="cleanup=\""+cleanup+"\"\n";
-    jobstring+="\n";
-    jobstring+="#File system info\n";
-    jobstring+="rootpath=\"${CRUNCH_ROOT}\"\n";
-    jobstring+="binpath=\"${rootpath}/bin\"\n";
-    jobstring+="scriptpath=\"${rootpath}/modules/modules/${modname}\"\n";
-    jobstring+="\n";
-    #jobstring+="#Script info\n";
-    #jobstring+="scriptlanguage=\""+scriptlanguage+"\"\n";
-    #jobstring+="scriptcommand=\""+scriptcommand+"\"\n";
-    #jobstring+="scriptflags=\""+scriptflags+"\"\n";
-    #jobstring+="scriptext=\""+scriptext+"\"\n";
-    #jobstring+="\n";
+    #    memoryperstep = maxmemorypernode
+    return ncores, nsteps, maxmemorypernode
+
+def writejobfile(reloadjob, modname, logging, cleanup, templocal, writelocal, writedb, statslocal, statsdb, markdone, jobname, jobstepnames, controllerpath, controllername, writemode, partitiontimelimit, buffertimelimit, partition, nnodes, counters, totmem, ncoresused, scriptlanguage, scriptcommand, scriptflags, scriptext, scriptargs, base, dbindexes, nthreadsfield, nbatch, nworkers, docbatches):
+    ndocbatches = len(docbatches)
+    outputlinemarkers = ["-", " + ", "&", "@", "CPUTime:", "MaxRSS:", "MaxVMSize:", "BSONSize:", "None"]
+    jobstring = "#!/bin/bash\n"
+    jobstring += "\n"
+    jobstring += "# Created " + str(datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")) + "\n"
+    jobstring += "\n"
+    jobstring += "#Job name\n"
+    jobstring += "#SBATCH -J \"" + jobname + "\"\n"
+    jobstring += "#################\n"
+    jobstring += "#Working directory\n"
+    jobstring += "#SBATCH -D \"" + controllerpath + "/jobs\"\n"
+    jobstring += "#################\n"
+    jobstring += "#Job output file\n"
+    jobstring += "#SBATCH -o \"" + jobname + ".log\"\n"
+    jobstring += "#################\n"
+    jobstring += "#Job error file\n"
+    jobstring += "#SBATCH -e \"" + jobname + ".err\"\n"
+    jobstring += "#################\n"
+    jobstring += "#Job file write mode\n"
+    jobstring += "#SBATCH --open-mode=\"" + writemode + "\"\n"
+    jobstring += "#################\n"
+    jobstring += "#Job max time\n"
+    jobstring += "#SBATCH --time=\"" + partitiontimelimit + "\"\n"
+    jobstring += "#################\n"
+    jobstring += "#Partition (queue) to use for job\n"
+    jobstring += "#SBATCH --partition=\"" + partition + "\"\n"
+    jobstring += "#################\n"
+    jobstring += "#Number of tasks (CPUs) allocated for job\n"
+    jobstring += "#SBATCH -n " + str(ncoresused) + "\n"
+    jobstring += "#################\n"
+    jobstring += "#Number of nodes to distribute n tasks across\n"
+    jobstring += "#SBATCH -N " + str(nnodes) + "\n"
+    jobstring += "#################\n"
+    #jobstring += "#Lock down N nodes for job\n"
+    #jobstring += "#SBATCH --exclusive\n"
+    #jobstring += "#################\n"
+    jobstring += "\n"
+    jobstring += "# Job info\n"
+    jobstring += "modname=\"" + modname + "\"\n"
+    jobstring += "controllername=\"" + controllername + "\"\n"
+    #jobstring += "outputlinemarkers=\"" + str(outputlinemarkers).replace(" ", "") + "\"\n"
+    #jobstring += "jobnum=" + str(counters[0]) + "\n"
+    jobstring += "nsteps=" + str(ncoresused) + "\n"
+    jobstring += "memunit=\"M\"\n"
+    jobstring += "totmem=" + str(totmem / 1000000) + "\n"
+    #jobstring += "stepmem=$((${totmem}/${nsteps}))\n"
+    jobstring += "steptime=\"" + buffertimelimit + "\"\n"
+    jobstring += "scriptlanguage=\"" + scriptlanguage + "\"\n"
+    jobstring += "nbatch=" + str(nbatch) + "\n"
+    jobstring += "nworkers=" + str(min(nworkers, ncoresused)) + "\n"
+    jobstring += "\n"
+    jobstring += "# Option info\n"
+    #jobstring += "logging=\"" + str(logging) + "\"\n"
+    #jobstring += "cleanup=\"" + str(cleanup) + "\"\n"
+    #jobstring += "templocal=\"" + str(templocal) + "\"\n"
+    #jobstring += "writelocal=\"" + str(writelocal) + "\"\n"
+    #jobstring += "writedb=\"" + str(writedb) + "\"\n"
+    #jobstring += "statslocal=\"" + str(statslocal) + "\"\n"
+    #jobstring += "statsdb=\"" + str(statsdb) + "\"\n"
+    jobstring += "markdone=\"" + markdone + "\"\n"
+    jobstring += "cleanup=\"" + cleanup + "\"\n"
+    jobstring += "\n"
+    jobstring += "# File system info\n"
+    jobstring += "rootpath=\"${CRUNCH_ROOT}\"\n"
+    jobstring += "binpath=\"${rootpath}/bin\"\n"
+    jobstring += "scriptpath=\"${rootpath}/modules/modules/${modname}\"\n"
+    jobstring += "\n"
+    #jobstring += "#Script info\n"
+    #jobstring += "scriptlanguage=\"" + scriptlanguage + "\"\n"
+    #jobstring += "scriptcommand=\"" + scriptcommand + "\"\n"
+    #jobstring += "scriptflags=\"" + scriptflags + "\"\n"
+    #jobstring += "scriptext=\"" + scriptext + "\"\n"
+    #jobstring += "\n"
     #if not reloadjob:
-    #    jobstring+="#Database info\n";
-    #    jobstring+="basecollection=\""+base+"\"\n";
-    #    jobstring+="dbindexes=\""+str([str(x) for x in dbindexes]).replace(" ","")+"\"\n";
-    #    jobstring+="\n";
-    jobstring+="#MPI info\n";
+    #    jobstring += "#Database info\n"
+    #    jobstring += "basecollection=\"" + base + "\"\n"
+    #    jobstring += "dbindexes=\"" + str([str(x) for x in dbindexes]).replace(" ", "") + "\"\n"
+    #    jobstring += "\n"
+    jobstring += "# MPI info\n"
     for i in range(ndocbatches):
-        with open(controllerpath+"/jobs/"+jobstepnames[i]+".docs","w") as docstream:
+        with open(controllerpath + "/jobs/" + jobstepnames[i] + ".docs", "w") as docstream:
             for doc in docbatches[i]:
                 if reloadjob:
-                    docstream.write(doc);
+                    docstream.write(doc)
                 else:
-                    docstream.write(json.dumps(doc,separators=(',',':'))+"\n");
+                    docstream.write(json.dumps(doc, separators = (',',':')) + "\n")
                 docstream.flush()
-        if nthreadsfield!="":
-            nstepthreads=max([x[nthreadsfield] for x in docbatches[i]]);
+        if nthreadsfield != "":
+            nstepthreads = max([x[nthreadsfield] for x in docbatches[i]])
         else:
-            nstepthreads=1;
-        jobstring+="nstepthreads="+str(nstepthreads)+"\n";
-        jobstring+="stepmem=$(((${totmem}*${nstepthreads})/${nsteps}))\n";
+            nstepthreads = 1
+        jobstring += "nstepthreads=" + str(nstepthreads) + "\n"
+        jobstring += "stepmem=$(((${totmem}*${nstepthreads})/${nsteps}))\n"
         if not reloadjob:
-            jobstring+="mpirun -srun -n \"${nstepthreads}\" -J \""+jobstepnames[i]+"\" --mem-per-cpu=\"${stepmem}${memunit}\" ";
-            if buffertimelimit!="infinite":
-                jobstring+="--time=\"${steptime}\" ";
-            jobstring+="python ${binpath}/wrapper.py --mod \"${modname}\" --controller \"${controllername}\" --stepid \"${SLURM_JOBID}."+str(i)+"\" --delay \"0.1\" --stats \"TotalCPUTime\" \"Rss\" \"Size\" --stats-delay \"0.01\" ";
-            if buffertimelimit!="infinite":
-                jobstring+="--time-limit \"${steptime}\" ";
-            jobstring+="--cleanup-after \"${cleanup}\" --nbatch \"${nbatch}\" --nworkers \"${nworkers}\" --random-nbatch --dbindexes "+" ".join(["\""+x+"\"" for x in dbindexes])+" --file \""+jobstepnames[i]+".docs\" ";
+            jobstring += "mpirun -srun -n \"${nstepthreads}\" -J \"" + jobstepnames[i] + "\" --mem-per-cpu=\"${stepmem}${memunit}\" "
+            if buffertimelimit != "infinite":
+                jobstring += "--time=\"${steptime}\" "
+            jobstring += "python ${binpath}/wrapper.py --mod \"${modname}\" --controller \"${controllername}\" --stepid \"${SLURM_JOBID}." + str(i) + "\" --delay \"0.1\" --stats \"TotalCPUTime\" \"Rss\" \"Size\" --stats-delay \"0.01\" "
+            if buffertimelimit != "infinite":
+                jobstring += "--time-limit \"${steptime}\" "
+            jobstring += "--cleanup-after \"${cleanup}\" --nbatch \"${nbatch}\" --nworkers \"${nworkers}\" --random-nbatch --dbindexes " + " ".join(["\"" + x + "\"" for x in dbindexes]) + " --file \"" + jobstepnames[i] + ".docs\" "
             if logging:
-                jobstring+="--logging ";
+                jobstring += "--logging "
             if templocal:
-                jobstring+="--temp-local ";
+                jobstring += "--temp-local "
             if writelocal:
-                jobstring+="--write-local ";
+                jobstring += "--write-local "
             if statslocal:
-                jobstring+="--stats-local ";
+                jobstring += "--stats-local "
             if writedb:
-                jobstring+="--write-db ";
+                jobstring += "--write-db "
             if statsdb:
-                jobstring+="--stats-db ";
-            jobstring+="--script-language \"${scriptlanguage}\" --script "+scriptcommand+scriptflags+"${scriptpath}/"+modname+scriptext+" --args "+scriptargs+" &";
-        jobstring+="\n";
-    jobstring+="wait";
+                jobstring += "--stats-db "
+            jobstring += "--script-language \"${scriptlanguage}\" --script " + scriptcommand + scriptflags + "${scriptpath}/" + modname + scriptext + " --args " + scriptargs + " &"
+        jobstring += "\n"
+    jobstring += "wait"
     #if reloadjob:
-    #    jobstring+="python \"${binpath}/reloadjobmanager.py\" \"${modname}\" \"${controllername}\" \"${scriptlanguage}\" \"${scriptcommand}\" \"${scriptflags}\" \"${scriptext}\" \"${outputlinemarkers}\" \"${SLURM_JOBID}\" \"${jobnum}\" \"${memunit}\" \"${totmem}\" \"${steptime}\" \"${nbatch}\" \"${nworkers}\" \"${logging}\" \"${cleanup}\" \"${templocal}\" \"${writelocal}\" \"${writedb}\" \"${statslocal}\" \"${statsdb}\" \"${markdone}\" \"${rootpath}\" \"${nstepthreads[@]}\"";
+    #    jobstring += "python \"${binpath}/reloadjobmanager.py\" \"${modname}\" \"${controllername}\" \"${scriptlanguage}\" \"${scriptcommand}\" \"${scriptflags}\" \"${scriptext}\" \"${outputlinemarkers}\" \"${SLURM_JOBID}\" \"${jobnum}\" \"${memunit}\" \"${totmem}\" \"${steptime}\" \"${nbatch}\" \"${nworkers}\" \"${logging}\" \"${cleanup}\" \"${templocal}\" \"${writelocal}\" \"${writedb}\" \"${statslocal}\" \"${statsdb}\" \"${markdone}\" \"${rootpath}\" \"${nstepthreads[@]}\""
     #else:
-    #    jobstring+="python \"${binpath}/queryjobmanager.py\" \"${modname}\" \"${controllername}\" \"${scriptlanguage}\" \"${scriptcommand}\" \"${scriptflags}\" \"${scriptext}\" \"${outputlinemarkers}\" \"${SLURM_JOBID}\" \"${jobnum}\" \"${memunit}\" \"${totmem}\" \"${steptime}\" \"${nbatch}\" \"${nworkers}\" \"${logging}\" \"${cleanup}\" \"${templocal}\" \"${writelocal}\" \"${writedb}\"\"${statslocal}\" \"${statsdb}\" \"${markdone}\" \"${rootpath}\" \"${basecollection}\" \"${dbindexes}\" \"${nstepthreads[@]}\"";
-    with open(controllerpath+"/jobs/"+jobname+".job","w") as jobstream:
-        jobstream.write(jobstring);
-        jobstream.flush();
-
-def waitforslots(reloadjob,storagelimit,needslicense,username,modname,controllername,controllerpath,querystatefilename,base,globalmaxjobcount,localmaxjobcount,localbinpath,licensescript,sublicensescript,maxthreads,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,dbindexes):
-    #print("dir_size/storagelimit: "+str(dir_size(controllerpath))+"/"+str(storagelimit));
-    #print("storageleft: "+str(storageleft(controllerpath,storagelimit)));
-    #sys.stdout.flush();
-    #needslicense=(licensestream!=None);
+    #    jobstring += "python \"${binpath}/queryjobmanager.py\" \"${modname}\" \"${controllername}\" \"${scriptlanguage}\" \"${scriptcommand}\" \"${scriptflags}\" \"${scriptext}\" \"${outputlinemarkers}\" \"${SLURM_JOBID}\" \"${jobnum}\" \"${memunit}\" \"${totmem}\" \"${steptime}\" \"${nbatch}\" \"${nworkers}\" \"${logging}\" \"${cleanup}\" \"${templocal}\" \"${writelocal}\" \"${writedb}\"\"${statslocal}\" \"${statsdb}\" \"${markdone}\" \"${rootpath}\" \"${basecollection}\" \"${dbindexes}\" \"${nstepthreads[@]}\""
+    with open(controllerpath + "/jobs/" + jobname + ".job", "w") as jobstream:
+        jobstream.write(jobstring)
+        jobstream.flush()
+
+def waitforslots(reloadjob, storagelimit, needslicense, username, modname, controllername, controllerpath, querystatefilename, base, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, maxthreads, starttime, controllerbuffertimelimit, statusstatefile, sleeptime, partitions, dbindexes):
+    #print("dir_size/storagelimit: " + str(dir_size(controllerpath)) + "/" + str(storagelimit))
+    #print("storageleft: " + str(storageleft(controllerpath, storagelimit)))
+    #sys.stdout.flush()
+    #needslicense = (licensestream != None)
     if needslicense:
-        jobslotsleft=clusterjobslotsleft(username,modname,controllername,globalmaxjobcount,localmaxjobcount);
-        nlicensesplit=licensecount(username,needslicense,localbinpath,licensescript,sublicensescript);
-        licensesleft=clusterlicensesleft(nlicensesplit,maxthreads);
-        orderedfreepartitions=orderfreepartitions(partitions);
-        releaseheldjobs(username,modname,controllername);
-        if (timeleft(starttime,controllerbuffertimelimit)>0) and not (jobslotsleft and licensesleft and (len(orderedfreepartitions)>0) and storageleft(controllerpath,storagelimit)):
-            with open(statusstatefile,"w") as statusstream:
-                statusstream.truncate(0);
-                statusstream.write("Waiting");
-                statusstream.flush();
-            while (timeleft(starttime,controllerbuffertimelimit)>0) and not (jobslotsleft and licensesleft and (len(orderedfreepartitions)>0) and storageleft(controllerpath,storagelimit)):
-                time.sleep(sleeptime);
-                jobslotsleft=clusterjobslotsleft(username,modname,controllername,globalmaxjobcount,localmaxjobcount);
-                nlicensesplit=licensecount(username,needslicense,localbinpath,licensescript,sublicensescript);
-                licensesleft=clusterlicensesleft(nlicensesplit,maxthreads);
-                orderedfreepartitions=orderfreepartitions(partitions);
-                releaseheldjobs(username,modname,controllername);
-        #fcntl.flock(licensestream,fcntl.LOCK_EX);
-        #fcntl.LOCK_EX might only work on files opened for writing. This one is open as "a+", so instead use bitwise OR with non-blocking and loop until lock is acquired.
-        while (timeleft(starttime,controllerbuffertimelimit)>0):
-            releaseheldjobs(username,modname,controllername);
+        jobslotsleft = clusterjobslotsleft(username, modname, controllername, globalmaxjobcount, localmaxjobcount)
+        nlicensesplit = licensecount(username, needslicense, localbinpath, licensescript, sublicensescript)
+        licensesleft = clusterlicensesleft(nlicensesplit, maxthreads)
+        orderedfreepartitions = orderfreepartitions(partitions)
+        releaseheldjobs(username, modname, controllername)
+        if (timeleft(starttime, controllerbuffertimelimit) > 0) and not (jobslotsleft and licensesleft and (len(orderedfreepartitions) > 0) and storageleft(controllerpath, storagelimit)):
+            with open(statusstatefile, "w") as statusstream:
+                statusstream.truncate(0)
+                statusstream.write("Waiting")
+                statusstream.flush()
+            while (timeleft(starttime, controllerbuffertimelimit) > 0) and not (jobslotsleft and licensesleft and (len(orderedfreepartitions) > 0) and storageleft(controllerpath, storagelimit)):
+                time.sleep(sleeptime)
+                jobslotsleft = clusterjobslotsleft(username, modname, controllername, globalmaxjobcount, localmaxjobcount)
+                nlicensesplit = licensecount(username, needslicense, localbinpath, licensescript, sublicensescript)
+                licensesleft = clusterlicensesleft(nlicensesplit, maxthreads)
+                orderedfreepartitions = orderfreepartitions(partitions)
+                releaseheldjobs(username, modname, controllername)
+        #fcntl.flock(licensestream, fcntl.LOCK_EX)
+        #fcntl.LOCK_EX might only work on files opened for writing. This one is open as "a + ", so instead use bitwise OR with non-blocking and loop until lock is acquired.
+        while (timeleft(starttime, controllerbuffertimelimit) > 0):
+            releaseheldjobs(username, modname, controllername)
             #try:
-            #    fcntl.flock(licensestream,fcntl.LOCK_EX | fcntl.LOCK_NB);
-            #    break;
+            #    fcntl.flock(licensestream, fcntl.LOCK_EX | fcntl.LOCK_NB)
+            #    break
             #except IOError as e:
-            #    if e.errno!=errno.EAGAIN:
-            #        raise;
+            #    if e.errno != errno.EAGAIN:
+            #        raise
             #    else:
-            #        time.sleep(0.1);
-        if not (timeleft(starttime,controllerbuffertimelimit)>0):
-            #print "hi";
-            #sys.stdout.flush();
-            return None;
-        #licensestream.seek(0,0);
-        #licenseheader=licensestream.readline();
-        #print licenseheader;
-        #sys.stdout.flush();
-        #licensestream.truncate(0);
-        #licensestream.seek(0,0);
-        #licensestream.write(licenseheader);
-        #licensestream.write(','.join([str(x) for x in nlicensesplit]));
-        #licensestream.flush();
+            #        time.sleep(0.1)
+        if not (timeleft(starttime, controllerbuffertimelimit) > 0):
+            #print "hi"
+            #sys.stdout.flush()
+            return None
+        #licensestream.seek(0, 0)
+        #licenseheader = licensestream.readline()
+        #print licenseheader
+        #sys.stdout.flush()
+        #licensestream.truncate(0)
+        #licensestream.seek(0, 0)
+        #licensestream.write(licenseheader)
+        #licensestream.write(', '.join([str(x) for x in nlicensesplit]))
+        #licensestream.flush()
     else:
-        jobslotsleft=clusterjobslotsleft(username,modname,controllername,globalmaxjobcount,localmaxjobcount);
-        orderedfreepartitions=orderfreepartitions(partitions);
-        releaseheldjobs(username,modname,controllername);
-        if (timeleft(starttime,controllerbuffertimelimit)>0) and not (jobslotsleft and (len(orderedfreepartitions)>0) and storageleft(controllerpath,storagelimit)):
-            with open(statusstatefile,"w") as statusstream:
-                statusstream.truncate(0);
-                statusstream.write("Waiting");
-                statusstream.flush();
-            while (timeleft(starttime,controllerbuffertimelimit)>0) and not (jobslotsleft and (len(orderedfreepartitions)>0) and storageleft(controllerpath,storagelimit)):
-                time.sleep(sleeptime);
-                jobslotsleft=clusterjobslotsleft(username,modname,controllername,globalmaxjobcount,localmaxjobcount);
-                orderedfreepartitions=orderfreepartitions(partitions);
-                releaseheldjobs(username,modname,controllername);
-        if not (timeleft(starttime,controllerbuffertimelimit)>0):
-            return None;
+        jobslotsleft = clusterjobslotsleft(username, modname, controllername, globalmaxjobcount, localmaxjobcount)
+        orderedfreepartitions = orderfreepartitions(partitions)
+        releaseheldjobs(username, modname, controllername)
+        if (timeleft(starttime, controllerbuffertimelimit) > 0) and not (jobslotsleft and (len(orderedfreepartitions) > 0) and storageleft(controllerpath, storagelimit)):
+            with open(statusstatefile, "w") as statusstream:
+                statusstream.truncate(0)
+                statusstream.write("Waiting")
+                statusstream.flush()
+            while (timeleft(starttime, controllerbuffertimelimit) > 0) and not (jobslotsleft and (len(orderedfreepartitions) > 0) and storageleft(controllerpath, storagelimit)):
+                time.sleep(sleeptime)
+                jobslotsleft = clusterjobslotsleft(username, modname, controllername, globalmaxjobcount, localmaxjobcount)
+                orderedfreepartitions = orderfreepartitions(partitions)
+                releaseheldjobs(username, modname, controllername)
+        if not (timeleft(starttime, controllerbuffertimelimit) > 0):
+            return None
 
     if reloadjob:
-        requeueskippedreloadjobs(modname,controllername,controllerpath,querystatefilename,base,counters,counterstatefile,counterheader,dbindexes);
+        requeueskippedreloadjobs(modname, controllername, controllerpath, querystatefilename, base, counters, counterstatefile, counterheader, dbindexes)
     else:
-        requeueskippedqueryjobs(modname,controllername,controllerpath,querystatefilename,base,counters,counterstatefile,counterheader,dbindexes);
+        requeueskippedqueryjobs(modname, controllername, controllerpath, querystatefilename, base, counters, counterstatefile, counterheader, dbindexes)
 
-    return orderedfreepartitions;
+    return orderedfreepartitions
 
-def doinput(docbatch,querylimit,counters,reloadjob,storagelimit,nthreadsfield,needslicense,username,modname,controllername,controllerpath,querystatefilename,base,globalmaxjobcount,localmaxjobcount,localbinpath,licensescript,sublicensescript,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,partitionsmaxmemory,scriptmemorylimit,localmaxstepcount,dbindexes,niters_orig):
-    if querylimit==None:
-        niters=niters_orig;
+def doinput(docbatch, querylimit, counters, reloadjob, storagelimit, nthreadsfield, needslicense, username, modname, controllername, controllerpath, querystatefilename, base, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, starttime, controllerbuffertimelimit, statusstatefile, sleeptime, partitions, partitionsmaxmemory, scriptmemorylimit, localmaxstepcount, dbindexes, niters_orig):
+    if querylimit == None:
+        niters = niters_orig
     else:
-        niters=min(niters_orig,querylimit-counters[1]+1);
-    if len(docbatch)==1:
-        niters=min(niters,len(docbatch[0]));
-    if (len(docbatch)>0) and (nthreadsfield!="") and (not reloadjob):
-        maxthreads=max([x[nthreadsfield] for x in docbatch[0:niters]]);
+        niters = min(niters_orig, querylimit-counters[1] + 1)
+    if len(docbatch) == 1:
+        niters = min(niters, len(docbatch[0]))
+    if (len(docbatch) > 0) and (nthreadsfield != "") and (not reloadjob):
+        maxthreads = max([x[nthreadsfield] for x in docbatch[0:niters]])
     else:
-        maxthreads=1;
-    orderedfreepartitions=waitforslots(reloadjob,storagelimit,needslicense,username,modname,controllername,controllerpath,querystatefilename,base,globalmaxjobcount,localmaxjobcount,localbinpath,licensescript,sublicensescript,maxthreads,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,dbindexes);
-    if orderedfreepartitions==None:
-        return None;
-    #orderedfreepartitions=orderpartitions(partitions);
-    nnodes=1;
-    i=0;
-    while i<len(orderedfreepartitions):
-        partition=orderedfreepartitions[i];
-        #nnodes=1;
-        distribution=distributeovernodes(partition,partitionsmaxmemory[partition],scriptmemorylimit,nnodes,localmaxstepcount,niters);
-        if distribution==None:
-            i+=1;
+        maxthreads = 1
+    orderedfreepartitions = waitforslots(reloadjob, storagelimit, needslicense, username, modname, controllername, controllerpath, querystatefilename, base, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, maxthreads, starttime, controllerbuffertimelimit, statusstatefile, sleeptime, partitions, dbindexes)
+    if orderedfreepartitions == None:
+        return None
+    #orderedfreepartitions = orderpartitions(partitions)
+    nnodes = 1
+    i = 0
+    while i < len(orderedfreepartitions):
+        partition = orderedfreepartitions[i]
+        #nnodes = 1
+        distribution = distributeovernodes(partition, partitionsmaxmemory[partition], scriptmemorylimit, nnodes, localmaxstepcount, niters)
+        if distribution == None:
+            i += 1
         else:
-            break;
-    if i==len(orderedfreepartitions):
-        raise Exception("Memory requirement is too large for this cluster.");
-    ncores,nsteps,maxmemorypernode=distribution;
-    if len(docbatch)>0:
-        maxthreads=0;
-        nextdocind=0;
-        if (nthreadsfield!="") and (not reloadjob):
-            while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
-                maxthreads+=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
-                nextdocind+=niters;
-            if nextdocind>len(docbatch):
-                nextdocind=len(docbatch);
-            if maxthreads>ncores:
-                nextdocind-=niters;
-                maxthreads-=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
+            break
+    if i == len(orderedfreepartitions):
+        raise Exception("Memory requirement is too large for this cluster.")
+    ncores, nsteps, maxmemorypernode = distribution
+    if len(docbatch) > 0:
+        maxthreads = 0
+        nextdocind = 0
+        if (nthreadsfield != "") and (not reloadjob):
+            while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
+                maxthreads += max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind + niters]])
+                nextdocind += niters
+            if nextdocind > len(docbatch):
+                nextdocind = len(docbatch)
+            if maxthreads > ncores:
+                nextdocind -= niters
+                maxthreads -= max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind + niters]])
         else:
-            while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
-                maxthreads+=1;
-                nextdocind+=niters;
-            if nextdocind>len(docbatch):
-                nextdocind=len(docbatch);
-            if maxthreads>ncores:
-                nextdocind-=niters;
-                maxthreads-=1;
-        while maxthreads==0:
-            nnodes+=1;
-            orderedfreepartitions=waitforslots(reloadjob,storagelimit,needslicense,username,modname,controllername,controllerpath,querystatefilename,base,globalmaxjobcount,localmaxjobcount,localbinpath,licensescript,sublicensescript,maxthreads,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,dbindexes);
-            if orderedfreepartitions==None:
-                return None;
-            #orderedfreepartitions=orderpartitions(partitions);
-            i=0;
-            while i<len(orderedfreepartitions):
-                partition=orderedfreepartitions[i];
-                #nnodes=1;
-                distribution=distributeovernodes(partition,partitionsmaxmemory[partition],scriptmemorylimit,nnodes,localmaxstepcount,niters);
-                if distribution==None:
-                    i+=1;
+            while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
+                maxthreads += 1
+                nextdocind += niters
+            if nextdocind > len(docbatch):
+                nextdocind = len(docbatch)
+            if maxthreads > ncores:
+                nextdocind -= niters
+                maxthreads -= 1
+        while maxthreads == 0:
+            nnodes += 1
+            orderedfreepartitions = waitforslots(reloadjob, storagelimit, needslicense, username, modname, controllername, controllerpath, querystatefilename, base, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, maxthreads, starttime, controllerbuffertimelimit, statusstatefile, sleeptime, partitions, dbindexes)
+            if orderedfreepartitions == None:
+                return None
+            #orderedfreepartitions = orderpartitions(partitions)
+            i = 0
+            while i < len(orderedfreepartitions):
+                partition = orderedfreepartitions[i]
+                #nnodes = 1
+                distribution = distributeovernodes(partition, partitionsmaxmemory[partition], scriptmemorylimit, nnodes, localmaxstepcount, niters)
+                if distribution == None:
+                    i += 1
                 else:
-                    break;
-            if i==len(orderedfreepartitions):
-                raise Exception("Memory requirement is too large for this cluster.");
-            ncores,nsteps,maxmemorypernode=distribution;
-            maxthreads=0;
-            nextdocind=0;
-            if (nthreadsfield!="") and (not reloadjob):
-                while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
-                    maxthreads+=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
-                    nextdocind+=niters;
-                if nextdocind>len(docbatch):
-                    nextdocind=len(docbatch);
-                if maxthreads>ncores:
-                    nextdocind-=niters;
-                    maxthreads-=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
+                    break
+            if i == len(orderedfreepartitions):
+                raise Exception("Memory requirement is too large for this cluster.")
+            ncores, nsteps, maxmemorypernode = distribution
+            maxthreads = 0
+            nextdocind = 0
+            if (nthreadsfield != "") and (not reloadjob):
+                while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
+                    maxthreads += max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind + niters]])
+                    nextdocind += niters
+                if nextdocind > len(docbatch):
+                    nextdocind = len(docbatch)
+                if maxthreads > ncores:
+                    nextdocind -= niters
+                    maxthreads -= max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind + niters]])
             else:
-                while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
-                    maxthreads+=1;
-                    nextdocind+=niters;
-                if nextdocind>len(docbatch):
-                    nextdocind=len(docbatch);
-                if maxthreads>ncores:
-                    nextdocind-=niters;
-                    maxthreads-=1;
-        #nsteps=0;
-    #print {"partition":partition,"nnodes":nnodes,"ncores":ncores,"nsteps":nsteps,"maxmemorypernode":maxmemorypernode};
-    #sys.stdout.flush();
-    return {"partition":partition,"nnodes":nnodes,"ncores":ncores,"nsteps":nsteps,"maxmemorypernode":maxmemorypernode};
-
-def doaction(counters,inputdoc,docbatch,querylimit,reloadjob,storagelimit,nthreadsfield,needslicense,username,globalmaxjobcount,localmaxjobcount,controllerpath,localbinpath,licensescript,sublicensescript,scriptlanguage,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,partitionsmaxmemory,scriptmemorylimit,localmaxstepcount,modname,controllername,dbindexes,logging,cleanup,emplocal,writelocal,writedb,statslocal,statsdb,markdone,writemode,scriptcommand,scriptflags,scriptext,scriptargs,querystatefilename,base,counterstatefile,counterheader,niters_orig,nbatch_orig,nworkers_orig):
-    partition=inputdoc['partition'];
-    nnodes=inputdoc['nnodes'];
-    ncores=inputdoc['ncores'];
-    nsteps=inputdoc['nsteps'];
-    maxmemorypernode=inputdoc["maxmemorypernode"];
-    #print(docbatch);
-    #sys.stdout.flush();
-    if querylimit==None:
-        niters=niters_orig;
+                while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
+                    maxthreads += 1
+                    nextdocind += niters
+                if nextdocind > len(docbatch):
+                    nextdocind = len(docbatch)
+                if maxthreads > ncores:
+                    nextdocind -= niters
+                    maxthreads -= 1
+        #nsteps = 0
+    #print {"partition": partition, "nnodes": nnodes, "ncores": ncores, "nsteps": nsteps, "maxmemorypernode": maxmemorypernode}
+    #sys.stdout.flush()
+    return {"partition": partition, "nnodes": nnodes, "ncores": ncores, "nsteps": nsteps, "maxmemorypernode": maxmemorypernode}
+
+def doaction(counters, inputdoc, docbatch, querylimit, reloadjob, storagelimit, nthreadsfield, needslicense, username, globalmaxjobcount, localmaxjobcount, controllerpath, localbinpath, licensescript, sublicensescript, scriptlanguage, starttime, controllerbuffertimelimit, statusstatefile, sleeptime, partitions, partitionsmaxmemory, scriptmemorylimit, localmaxstepcount, modname, controllername, dbindexes, logging, cleanup, emplocal, writelocal, writedb, statslocal, statsdb, markdone, writemode, scriptcommand, scriptflags, scriptext, scriptargs, querystatefilename, base, counterstatefile, counterheader, niters_orig, nbatch_orig, nworkers_orig):
+    partition = inputdoc['partition']
+    nnodes = inputdoc['nnodes']
+    ncores = inputdoc['ncores']
+    nsteps = inputdoc['nsteps']
+    maxmemorypernode = inputdoc["maxmemorypernode"]
+    #print(docbatch)
+    #sys.stdout.flush()
+    if querylimit == None:
+        niters = niters_orig
     else:
-        niters=min(niters_orig,querylimit-counters[1]+1);
-    if len(docbatch)<nsteps:
-        niters=min(niters,len(docbatch));
-    if nbatch_orig>niters:
-        nbatch=niters;
+        niters = min(niters_orig, querylimit-counters[1] + 1)
+    if len(docbatch) < nsteps:
+        niters = min(niters, len(docbatch))
+    if nbatch_orig > niters:
+        nbatch = niters
     else:
-        nbatch=nbatch_orig;
-    ndocs=len(docbatch);
-    if nworkers_orig*nbatch>ndocs:
-        nworkers=int(ndocs/nbatch)+int(ndocs%nbatch>0);
+        nbatch = nbatch_orig
+    ndocs = len(docbatch)
+    if nworkers_orig * nbatch > ndocs:
+        nworkers = int(ndocs / nbatch) + int(ndocs % nbatch > 0)
     else:
-        nworkers=nworkers_orig;
-    maxthreads=0;
-    nextdocind=0;
-    if (nthreadsfield!="") and (not reloadjob):
-        while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
-            maxthreads+=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
-            nextdocind+=niters;
-        if nextdocind>len(docbatch):
-            nextdocind=len(docbatch);
-        if maxthreads>ncores:
-            nextdocind-=niters;
-            maxthreads-=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
+        nworkers = nworkers_orig
+    maxthreads = 0
+    nextdocind = 0
+    if (nthreadsfield != "") and (not reloadjob):
+        while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
+            maxthreads += max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind + niters]])
+            nextdocind += niters
+        if nextdocind > len(docbatch):
+            nextdocind = len(docbatch)
+        if maxthreads > ncores:
+            nextdocind -= niters
+            maxthreads -= max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind + niters]])
     else:
-        while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
-            maxthreads+=1;
-            nextdocind+=niters;
-        if nextdocind>len(docbatch):
-            nextdocind=len(docbatch);
-        if maxthreads>ncores:
-            nextdocind-=niters;
-            maxthreads-=1;
-    while maxthreads==0:
-        nnodes+=1;
-        orderedfreepartitions=waitforslots(reloadjob,storagelimit,needslicense,username,modname,controllername,controllerpath,querystatefilename,base,globalmaxjobcount,localmaxjobcount,localbinpath,licensescript,sublicensescript,maxthreads,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,dbindexes);
-        if orderedfreepartitions==None:
-            return None;
-        #orderedfreepartitions=orderpartitions(partitions);
-        i=0;
-        while i<len(orderedfreepartitions):
-            partition=orderedfreepartitions[i];
-            #nnodes=1;
-            distribution=distributeovernodes(partition,partitionsmaxmemory[partition],scriptmemorylimit,nnodes,localmaxstepcount,niters);
-            if distribution==None:
-                i+=1;
+        while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
+            maxthreads += 1
+            nextdocind += niters
+        if nextdocind > len(docbatch):
+            nextdocind = len(docbatch)
+        if maxthreads > ncores:
+            nextdocind -= niters
+            maxthreads -= 1
+    while maxthreads == 0:
+        nnodes += 1
+        orderedfreepartitions = waitforslots(reloadjob, storagelimit, needslicense, username, modname, controllername, controllerpath, querystatefilename, base, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, maxthreads, starttime, controllerbuffertimelimit, statusstatefile, sleeptime, partitions, dbindexes)
+        if orderedfreepartitions == None:
+            return None
+        #orderedfreepartitions = orderpartitions(partitions)
+        i = 0
+        while i < len(orderedfreepartitions):
+            partition = orderedfreepartitions[i]
+            #nnodes = 1
+            distribution = distributeovernodes(partition, partitionsmaxmemory[partition], scriptmemorylimit, nnodes, localmaxstepcount, niters)
+            if distribution == None:
+                i += 1
             else:
-                break;
-        if i==len(orderedfreepartitions):
-            raise Exception("Memory requirement is too large for this cluster.");
-        ncores,nsteps,maxmemorypernode=distribution;
-        maxthreads=0;
-        nextdocind=0;
-        if (nthreadsfield!="") and (not reloadjob):
-            while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
-                maxthreads+=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
-                nextdocind+=niters;
-            if nextdocind>len(docbatch):
-                nextdocind=len(docbatch);
-            if maxthreads>ncores:
-                nextdocind-=niters;
-                maxthreads-=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
+                break
+        if i == len(orderedfreepartitions):
+            raise Exception("Memory requirement is too large for this cluster.")
+        ncores, nsteps, maxmemorypernode = distribution
+        maxthreads = 0
+        nextdocind = 0
+        if (nthreadsfield != "") and (not reloadjob):
+            while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
+                maxthreads += max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind + niters]])
+                nextdocind += niters
+            if nextdocind > len(docbatch):
+                nextdocind = len(docbatch)
+            if maxthreads > ncores:
+                nextdocind -= niters
+                maxthreads -= max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind + niters]])
         else:
-            while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
-                maxthreads+=1;
-                nextdocind+=niters;
-            if nextdocind>len(docbatch):
-                nextdocind=len(docbatch);
-            if maxthreads>ncores:
-                nextdocind-=niters;
-                maxthreads-=1;
-    #docbatchwrite=docbatch[:nextdocind];
-    docbatchwrite=[docbatch[i:i+niters] for i in range(0,nextdocind,niters)];
-    #docbatchpass=docbatch[nextdocind:];
-
-    totmem=nnodes*maxmemorypernode;
-    ncoresused=len(docbatchwrite);
-    memoryperstep=totmem/ncoresused;
-    #niters=len(docbatch);
-    #if nthreadsfield=="":
-    #    totnthreadsfield=niters;
+            while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
+                maxthreads += 1
+                nextdocind += niters
+            if nextdocind > len(docbatch):
+                nextdocind = len(docbatch)
+            if maxthreads > ncores:
+                nextdocind -= niters
+                maxthreads -= 1
+    #docbatchwrite = docbatch[:nextdocind]
+    docbatchwrite = [docbatch[i:i + niters] for i in range(0, nextdocind, niters)]
+    #docbatchpass = docbatch[nextdocind:]
+
+    totmem = nnodes * maxmemorypernode
+    ncoresused = len(docbatchwrite)
+    memoryperstep = totmem / ncoresused
+    #niters = len(docbatch)
+    #if nthreadsfield == "":
+    #    totnthreadsfield = niters
     #else:
-    #    totnthreadsfield=sum([x[nthreadsfield] for x in docbatch]);
-    #while not clusterjobslotsleft(globalmaxjobcount,scriptext,minnsteps=inputdoc["nsteps"]):
-    #    time.sleep(sleeptime);
-    #doc=json.loads(doc.rstrip('\n'));
-    #if niters==1:
-    #    jobstepnames=[indexdoc2jobstepname(x,modname,controllername,dbindexes) for x in docbatchwrite];
+    #    totnthreadsfield = sum([x[nthreadsfield] for x in docbatch])
+    #while not clusterjobslotsleft(globalmaxjobcount, scriptext, minnsteps = inputdoc["nsteps"]):
+    #    time.sleep(sleeptime)
+    #doc = json.loads(doc.rstrip('\n'))
+    #if niters == 1:
+    #    jobstepnames = [indexdoc2jobstepname(x, modname, controllername, dbindexes) for x in docbatchwrite]
     #else:
-    jobstepnames=["crunch_"+modname+"_"+controllername+"_job_"+str(counters[0])+"_step_"+str(i+1) for i in range(len(docbatchwrite))];
-    #jobstepnamescontract=jobstepnamescontract(jobstepnames);
-    jobname="crunch_"+modname+"_"+controllername+"_job_"+str(counters[0])+"_steps_"+str((counters[1]+niters-1)/niters)+"-"+str((counters[1]+niters*len(docbatchwrite)-1)/niters);
+    jobstepnames = ["crunch_" + modname + "_" + controllername + "_job_" + str(counters[0]) + "_step_" + str(i + 1) for i in range(len(docbatchwrite))]
+    #jobstepnamescontract = jobstepnamescontract(jobstepnames)
+    jobname = "crunch_" + modname + "_" + controllername + "_job_" + str(counters[0]) + "_steps_" + str((counters[1] + niters-1) / niters) + "-" + str((counters[1] + niters * len(docbatchwrite)-1) / niters)
     #if reloadjob:
-    #    jobstepnames=["reload_"+x for x in jobstepnames];
-    #    jobname="reload_"+jobname;
-    partitiontimelimit,buffertimelimit=getpartitiontimelimit(partition,scripttimelimit,scriptbuffertime);
-    #if len(docbatch)<inputdoc["nsteps"]:
-    #    inputdoc["memoryperstep"]=(memoryperstep*inputdoc["nsteps"])/len(docbatch);
-    writejobfile(reloadjob,modname,logging,cleanup,templocal,writelocal,writedb,statslocal,statsdb,markdone,jobname,jobstepnames,controllerpath,controllername,writemode,partitiontimelimit,buffertimelimit,partition,nnodes,counters,totmem,ncoresused,scriptlanguage,scriptcommand,scriptflags,scriptext,scriptargs,base,dbindexes,nthreadsfield,nbatch,nworkers,docbatchwrite);
+    #    jobstepnames = ["reload_" + x for x in jobstepnames]
+    #    jobname = "reload_" + jobname
+    partitiontimelimit, buffertimelimit = getpartitiontimelimit(partition, scripttimelimit, scriptbuffertime)
+    #if len(docbatch) < inputdoc["nsteps"]:
+    #    inputdoc["memoryperstep"] = (memoryperstep * inputdoc["nsteps"]) / len(docbatch)
+    writejobfile(reloadjob, modname, logging, cleanup, templocal, writelocal, writedb, statslocal, statsdb, markdone, jobname, jobstepnames, controllerpath, controllername, writemode, partitiontimelimit, buffertimelimit, partition, nnodes, counters, totmem, ncoresused, scriptlanguage, scriptcommand, scriptflags, scriptext, scriptargs, base, dbindexes, nthreadsfield, nbatch, nworkers, docbatchwrite)
     #Submit job file
-    if (nthreadsfield!="") and (not reloadjob):
-        nthreads=[max([y[nthreadsfield] for y in x]) for x in docbatchwrite];
+    if (nthreadsfield != "") and (not reloadjob):
+        nthreads = [max([y[nthreadsfield] for y in x]) for x in docbatchwrite]
     else:
-        nthreads=[1 for x in docbatchwrite];
-    submitjob(workpath,jobname,jobstepnames,nnodes,ncoresused,nthreads,niters,nbatch,partition,memoryperstep,maxmemorypernode,resubmit=False);
-    #needslicense=(licensestream!=None);
+        nthreads = [1 for x in docbatchwrite]
+    submitjob(workpath, jobname, jobstepnames, nnodes, ncoresused, nthreads, niters, nbatch, partition, memoryperstep, maxmemorypernode, resubmit = False)
+    #needslicense = (licensestream != None)
     #if needslicense:
-    #    fcntl.flock(licensestream,fcntl.LOCK_UN);
-        #pendlicensestream.seek(0,0);
-        #pendlicenseheader=pendlicensestream.readline();
-        #npendlicensesplit=[eval(x) for x in pendlicensestream.readline().rstrip("\n").split(",")];
-        #print npendlicensesplit;
-        #pendlicensestream.truncate(0);
-        #print "hi";
-        #pendlicensestream.seek(0,0);
-        #pendlicensestream.write(pendlicenseheader);
-        #npendlicenses=npendlicensesplit[0];
-        #if len(npendlicensesplit)>1:
-        #    npendsublicenses=npendlicensesplit[1];
-        #    pendlicensestream.write(str(npendlicenses+niters)+","+str(npendsublicenses+totnthreadsfield));
+    #    fcntl.flock(licensestream, fcntl.LOCK_UN)
+        #pendlicensestream.seek(0, 0)
+        #pendlicenseheader = pendlicensestream.readline()
+        #npendlicensesplit = [eval(x) for x in pendlicensestream.readline().rstrip("\n").split(",")]
+        #print npendlicensesplit
+        #pendlicensestream.truncate(0)
+        #print "hi"
+        #pendlicensestream.seek(0, 0)
+        #pendlicensestream.write(pendlicenseheader)
+        #npendlicenses = npendlicensesplit[0]
+        #if len(npendlicensesplit) > 1:
+        #    npendsublicenses = npendlicensesplit[1]
+        #    pendlicensestream.write(str(npendlicenses + niters) + ", " + str(npendsublicenses + totnthreadsfield))
         #else:
-        #    pendlicensestream.write(str(npendlicenses+niters));
-    with open(statusstatefile,"w") as statusstream:
-        statusstream.truncate(0);
-        statusstream.write("Running");
-        statusstream.flush();
-    #releaseheldjobs(username,modname,controllername);
-    #print "End action";
-    #sys.stdout.flush();
-    #seekstream.write(querystream.tell());
-    #seekstream.flush();
-    #seekstream.seek(0);
-    #doc=querystream.readline();
-    releaseheldjobs(username,modname,controllername);
-    return nextdocind;#docbatchpass;
-
-def docounterupdate(counters,counterstatefile,counterheader):
-    with open(counterstatefile,"w") as counterstream:
-        counterstream.write(counterheader);
-        counterstream.write(str(counters[0])+","+str(counters[1]));
-        counterstream.flush();
+        #    pendlicensestream.write(str(npendlicenses + niters))
+    with open(statusstatefile, "w") as statusstream:
+        statusstream.truncate(0)
+        statusstream.write("Running")
+        statusstream.flush()
+    #releaseheldjobs(username, modname, controllername)
+    #print "End action"
+    #sys.stdout.flush()
+    #seekstream.write(querystream.tell())
+    #seekstream.flush()
+    #seekstream.seek(0)
+    #doc = querystream.readline()
+    releaseheldjobs(username, modname, controllername)
+    return nextdocind;#docbatchpass
+
+def docounterupdate(counters, counterstatefile, counterheader):
+    with open(counterstatefile, "w") as counterstream:
+        counterstream.write(counterheader)
+        counterstream.write(str(counters[0]) + ", " + str(counters[1]))
+        counterstream.flush()
 
 try:
-    #print "main";
-    #print "scontrol show config | grep 'MaxJobCount\|MaxStepCount' | sed 's/\s//g' | cut -d'=' -f2 | tr '\n' ',' | head -c -1";
-    #print "";
-    #sys.stdout.flush();
+    #print "main"
+    #print "scontrol show config | grep 'MaxJobCount\|MaxStepCount' | sed 's/\s//g' | cut -d'=' -f2 | tr '\n' ', ' | head -c -1"
+    #print ""
+    #sys.stdout.flush()
 
     #Cluster info
-    username=os.environ['USER'];
-    rootpath=os.environ['CRUNCH_ROOT'];
-    localbinpath=os.environ['USER_LOCAL'];
-    #username,rootpath=Popen("echo \"${USER},${CRUNCH_ROOT}\" | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(",");
+    username = os.environ['USER']
+    rootpath = os.environ['CRUNCH_ROOT']
+    localbinpath = os.environ['USER_LOCAL']
+    #username, rootpath = Popen("echo \"${USER},${CRUNCH_ROOT}\" | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(",")
 
-    with open(rootpath+"/config.yaml","r") as configstream:
-        configdoc=yaml.load(configstream);
+    with open(rootpath + "/config.yaml", "r") as configstream:
+        configdoc = yaml.load(configstream)
 
-    if configdoc["workload-manager"]=="slurm":
-        from crunch_slurm import *;
+    if configdoc["workload-manager"] == "slurm":
+        from crunch_slurm import *
 
-    globalmaxjobcount=get_maxjobcount();
-    localmaxstepcount=get_maxstepcount();
+    globalmaxjobcount = get_maxjobcount()
+    localmaxstepcount = get_maxstepcount()
 
     #Input controller info
-    modname=sys.argv[1];
-    controllername=sys.argv[2];
-    controllerjobid=sys.argv[3];
-    #controllerpartition=sys.argv[4];
-    controllerbuffertime=sys.argv[4];
-    storagelimit=sys.argv[5];
-    #largemempartitions=sys.argv[6].split(",");
-    sleeptime=eval(sys.argv[6]);
+    modname = sys.argv[1]
+    controllername = sys.argv[2]
+    controllerjobid = sys.argv[3]
+    #controllerpartition = sys.argv[4]
+    controllerbuffertime = sys.argv[4]
+    storagelimit = sys.argv[5]
+    #largemempartitions = sys.argv[6].split(",")
+    sleeptime = eval(sys.argv[6])
 
-    #seekfile=sys.argv[7]; 
+    #seekfile = sys.argv[7]; 
 
     #Input path info
-    #rootpath=sys.argv[7];
-    #rootpath=sys.argv[8];
-    #binpath=sys.argv[9];
+    #rootpath = sys.argv[7]
+    #rootpath = sys.argv[8]
+    #binpath = sys.argv[9]
 
     #Input database info
-    dbtype=sys.argv[7];
-    dbusername=sys.argv[8];
-    dbpassword=sys.argv[9];
-    dbhost=sys.argv[10];
-    dbport=sys.argv[11];
-    dbname=sys.argv[12];
-    queries=eval(sys.argv[13]);
-    #dumpfile=sys.argv[13];
-    basecollection=sys.argv[14];
-    nthreadsfield=sys.argv[15];
+    dbtype = sys.argv[7]
+    dbusername = sys.argv[8]
+    dbpassword = sys.argv[9]
+    dbhost = sys.argv[10]
+    dbport = sys.argv[11]
+    dbname = sys.argv[12]
+    queries = eval(sys.argv[13])
+    #dumpfile = sys.argv[13]
+    basecollection = sys.argv[14]
+    nthreadsfield = sys.argv[15]
 
     #Input script info
-    scriptlanguage=sys.argv[16];
-    scriptargs=sys.argv[17];
-    partitions=sys.argv[18].split(",");
-    writemode=sys.argv[19];
-    #scripttimelimit=timestamp2unit(sys.argv[15]);
-    scriptmemorylimit=sys.argv[20];
-    scripttimelimit=sys.argv[21];
-    scriptbuffertime=sys.argv[22];
-    localmaxjobcountstr=sys.argv[23];
-    #outputlinemarkers=sys.argv[15].split(",");
-    #newcollection,newfield=sys.argv[18].split(",");
+    scriptlanguage = sys.argv[16]
+    scriptargs = sys.argv[17]
+    partitions = sys.argv[18].split(",")
+    writemode = sys.argv[19]
+    #scripttimelimit = timestamp2unit(sys.argv[15])
+    scriptmemorylimit = sys.argv[20]
+    scripttimelimit = sys.argv[21]
+    scriptbuffertime = sys.argv[22]
+    localmaxjobcountstr = sys.argv[23]
+    #outputlinemarkers = sys.argv[15].split(",")
+    #newcollection, newfield = sys.argv[18].split(",")
 
     #Options
-    blocking=eval(sys.argv[24]);
-    logging=eval(sys.argv[25]);
-    templocal=eval(sys.argv[26])
-    writelocal=eval(sys.argv[27]);
-    writedb=eval(sys.argv[28]);
-    statslocal=eval(sys.argv[29]);
-    statsdb=eval(sys.argv[30]);
-    markdone=sys.argv[31];
-    cleanup=sys.argv[32];
-    niters_orig=eval(sys.argv[33]);
-    nbatch_orig=eval(sys.argv[34]);
-    nworkers_orig=eval(sys.argv[35]);
+    blocking = eval(sys.argv[24])
+    logging = eval(sys.argv[25])
+    templocal = eval(sys.argv[26])
+    writelocal = eval(sys.argv[27])
+    writedb = eval(sys.argv[28])
+    statslocal = eval(sys.argv[29])
+    statsdb = eval(sys.argv[30])
+    markdone = sys.argv[31]
+    cleanup = sys.argv[32]
+    niters_orig = eval(sys.argv[33])
+    nbatch_orig = eval(sys.argv[34])
+    nworkers_orig = eval(sys.argv[35])
     
     #Read seek position from file
-    #with open(controllerpath+"/"+seekfile,"r") as seekstream:
-    #    seekpos=eval(seekstream.read());
+    #with open(controllerpath + "/" + seekfile, "r") as seekstream:
+    #    seekpos = eval(seekstream.read())
 
     #Open seek file stream
-    #seekstream=open(controllerpath+"/"+seekfile,"w");
+    #seekstream = open(controllerpath + "/" + seekfile, "w")
 
     #If first submission, read from database
-    #if seekpos==-1:
+    #if seekpos == -1:
     #Open connection to remote database
-    #sys.stdout.flush();
+    #sys.stdout.flush()
 
-    if storagelimit=="":
-        storagelimit=None;
+    if storagelimit == "":
+        storagelimit = None
     else:
-        storagelimit_num="";
-        storagelimit_unit="";
+        storagelimit_num = ""
+        storagelimit_unit = ""
         for x in storagelimit:
-            if x.isdigit() or x==".":
-                storagelimit_num+=x;
+            if x.isdigit() or x == ".":
+                storagelimit_num += x
             else:
-                storagelimit_unit+=x;
-        if storagelimit_unit.lower()=="b":
-            storagelimit=float(storagelimit_num);
-        elif storagelimit_unit.lower() in ["k","kb"]:
-            storagelimit=float(storagelimit_num)*1024;
-        elif storagelimit_unit.lower() in ["m","mb"]:
-            storagelimit=float(storagelimit_num)*1000*1024;
-        elif storagelimit_unit.lower() in ["g","gb"]:
-            storagelimit=float(storagelimit_num)*1000*1000*1024;
-
-    controllerstats=get_controllerstats(controllerjobid);
-    while len(controllerstats)<4:
-        time.sleep(sleeptime);
-        controllerstats=get_controllerstats(controllerjobid);
-    controllerpartition,controllertimelimit,controllernnodes,controllerncores=controllerstats;
-
-    print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
-    print "Starting job crunch_"+modname+"_"+controllername+"_controller";
-    print "";
-    print "";
-    sys.stdout.flush();
-
-    controllerpath=get_controllerpath(controllerjobid);
-
-    #statepath=rootpath+"/state";
-    binpath=rootpath+"/bin";
-    workpath=controllerpath+"/jobs";
+                storagelimit_unit += x
+        if storagelimit_unit.lower() == "b":
+            storagelimit = float(storagelimit_num)
+        elif storagelimit_unit.lower() in ["k", "kb"]:
+            storagelimit = float(storagelimit_num) * 1024
+        elif storagelimit_unit.lower() in ["m", "mb"]:
+            storagelimit = float(storagelimit_num) * 1000 * 1024
+        elif storagelimit_unit.lower() in ["g", "gb"]:
+            storagelimit = float(storagelimit_num) * 1000 * 1000 * 1024
+
+    controllerstats = get_controllerstats(controllerjobid)
+    while len(controllerstats) < 4:
+        time.sleep(sleeptime)
+        controllerstats = get_controllerstats(controllerjobid)
+    controllerpartition, controllertimelimit, controllernnodes, controllerncores = controllerstats
+
+    print datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")
+    print "Starting job crunch_" + modname + "_" + controllername + "_controller"
+    print ""
+    print ""
+    sys.stdout.flush()
+
+    controllerpath = get_controllerpath(controllerjobid)
+
+    #statepath = rootpath + "/state"
+    binpath = rootpath + "/bin"
+    workpath = controllerpath + "/jobs"
     if not os.path.isdir(workpath):
-        os.mkdir(workpath);
-    dependenciesfile=rootpath+"/modules/modules/"+modname+"/dependencies";
-    #resourcesstatefile=statepath+"/resources";
-    #softwarestatefile=statepath+"/software";
-    #globalmaxjobsfile=statepath+"/maxjobs";
-    counterstatefile=controllerpath+"/batchcounter";
-    statusstatefile=controllerpath+"/status";
+        os.mkdir(workpath)
+    dependenciesfile = rootpath + "/modules/modules/" + modname + "/dependencies"
+    #resourcesstatefile = statepath + "/resources"
+    #softwarestatefile = statepath + "/software"
+    #globalmaxjobsfile = statepath + "/maxjobs"
+    counterstatefile = controllerpath + "/batchcounter"
+    statusstatefile = controllerpath + "/status"
     
-    #querystatefile=controllerpath+"/querystate";
+    #querystatefile = controllerpath + "/querystate"
 
-    for f in glob.iglob(workpath+"/*.lock"):
-        os.remove(f);
+    for f in glob.iglob(workpath + "/*.lock"):
+        os.remove(f)
 
-    with open(statusstatefile,"w") as statusstream:
-        statusstream.truncate(0);
-        statusstream.write("Starting");
+    with open(statusstatefile, "w") as statusstream:
+        statusstream.truncate(0)
+        statusstream.write("Starting")
 
-    assert isinstance(configdoc["max-jobs"],int) or configdoc["max-jobs"] is None;
-    if isinstance(configdoc["max-jobs"],int):
-        globalmaxjobcount=min(configdoc["max-jobs"],globalmaxjobcount);
-    assert isinstance(configdoc["max-steps"],int) or configdoc["max-steps"] is None;
-    if isinstance(configdoc["max-steps"],int):
-        localmaxstepcount=min(configdoc["max-steps"],localmaxstepcount);
+    assert isinstance(configdoc["max-jobs"], int) or configdoc["max-jobs"] is None
+    if isinstance(configdoc["max-jobs"], int):
+        globalmaxjobcount = min(configdoc["max-jobs"], globalmaxjobcount)
+    assert isinstance(configdoc["max-steps"], int) or configdoc["max-steps"] is None
+    if isinstance(configdoc["max-steps"], int):
+        localmaxstepcount = min(configdoc["max-steps"], localmaxstepcount)
 
-    if localmaxjobcountstr=="":
-        localmaxjobcount=globalmaxjobcount;
+    if localmaxjobcountstr == "":
+        localmaxjobcount = globalmaxjobcount
     else:
-        localmaxjobcount=min(eval(localmaxjobcountstr),globalmaxjobcount);
+        localmaxjobcount = min(eval(localmaxjobcountstr), globalmaxjobcount)
 
-    if scriptbuffertime=="":
-        scriptbuffertime="00:00:00";
+    if scriptbuffertime == "":
+        scriptbuffertime = "00:00:00"
 
-    controllerpartitiontimelimit,controllerbuffertimelimit=getpartitiontimelimit(controllerpartition,controllertimelimit,controllerbuffertime);
+    controllerpartitiontimelimit, controllerbuffertimelimit = getpartitiontimelimit(controllerpartition, controllertimelimit, controllerbuffertime)
 
-    if dbtype=="mongodb":
-        import mongolink;
-        if dbusername==None:
-            dbclient=mongolink.MongoClient("mongodb://"+dbhost+":"+dbport+"/"+dbname);
+    if dbtype == "mongodb":
+        import mongojoin
+        if dbusername == None:
+            dbclient = mongojoin.MongoClient("mongodb://" + dbhost + ":" + dbport + "/" + dbname)
         else:
-            dbclient=mongolink.MongoClient("mongodb://"+dbusername+":"+dbpassword+"@"+dbhost+":"+dbport+"/"+dbname+"?authMechanism=SCRAM-SHA-1");
+            dbclient = mongojoin.MongoClient("mongodb://" + dbusername + ":" + dbpassword + "@" + dbhost + ":" + dbport + "/" + dbname + "?authMechanism=SCRAM-SHA-1")
 
-        #dbname=mongouri.split("/")[-1];
-        db=dbclient[dbname];
+        #dbname = mongouri.split("/")[-1]
+        db = dbclient[dbname]
 
-        dbindexes=mongolink.getintersectionindexes(db,basecollection);
-        #print dbindexes;
-        #sys.stdout.flush();
+        dbindexes = mongojoin.getintersectionindexes(db, basecollection)
+        #print dbindexes
+        #sys.stdout.flush()
 
-        allindexes=mongolink.getunionindexes(db);
+        allindexes = mongojoin.getunionindexes(db)
     else:
-        raise Exception("Only \"mongodb\" is currently supported.");
+        raise Exception("Only \"mongodb\" is currently supported.")
 
-    scriptext=(configdoc["software"][scriptlanguage]["extension"] if "extension" in configdoc["software"][scriptlanguage].keys() else "");
-    scriptcommand=(configdoc["software"][scriptlanguage]["command"]+" " if "command" in configdoc["software"][scriptlanguage].keys() else "");
-    scriptflags=(" ".join(configdoc["software"][scriptlanguage]["flags"])+" " if "flags" in configdoc["software"][scriptlanguage].keys() else "");
-    needslicense=("license-command" in configdoc["software"][scriptlanguage].keys());
+    scriptext = (configdoc["software"][scriptlanguage]["extension"] if "extension" in configdoc["software"][scriptlanguage].keys() else "")
+    scriptcommand = (configdoc["software"][scriptlanguage]["command"] + " " if "command" in configdoc["software"][scriptlanguage].keys() else "")
+    scriptflags = (" ".join(configdoc["software"][scriptlanguage]["flags"]) + " " if "flags" in configdoc["software"][scriptlanguage].keys() else "")
+    needslicense = ("license-command" in configdoc["software"][scriptlanguage].keys())
     if needslicense:
-        licensescript=configdoc["software"][scriptlanguage]["license-command"];
-        sublicensescript=(configdoc["software"][scriptlanguage]["sublicense-command"]+" " if "sublicense-command" in configdoc["software"][scriptlanguage].keys() else None);
-        licensestatefile=localbinpath+"/"+licensescript;
-        #licensestream=open(licensestatefile,"a+");
+        licensescript = configdoc["software"][scriptlanguage]["license-command"]
+        sublicensescript = (configdoc["software"][scriptlanguage]["sublicense-command"] + " " if "sublicense-command" in configdoc["software"][scriptlanguage].keys() else None)
+        licensestatefile = localbinpath + "/" + licensescript
+        #licensestream = open(licensestatefile, "a + ")
     else:
-        licensescript=None;
-        sublicensescript=None;
-        #licensestream=None;
+        licensescript = None
+        sublicensescript = None
+        #licensestream = None
 
-    assert "resources" in configdoc.keys();
-    partitionsmaxmemory=configdoc["resources"];
+    assert "resources" in configdoc.keys()
+    partitionsmaxmemory = configdoc["resources"]
     
     try:
-        with open(dependenciesfile,"r") as dependenciesstream:
-            dependencies=[x.rstrip('\n') for x in dependenciesstream.readlines()];
+        with open(dependenciesfile, "r") as dependenciesstream:
+            dependencies = [x.rstrip('\n') for x in dependenciesstream.readlines()]
     except IOError:
-        print "File path \""+dependenciesfile+"\" does not exist.";
-        sys.stdout.flush();
-        raise;
+        print "File path \"" + dependenciesfile + "\" does not exist."
+        sys.stdout.flush()
+        raise
 
-    #dependencies=modlist[:modlist.index(modname)];
+    #dependencies = modlist[:modlist.index(modname)]
 
     #if firstlastrun and needslicense:
-    #   fcntl.flock(pendlicensestream,fcntl.LOCK_UN);
-    #firstrun=True;
+    #   fcntl.flock(pendlicensestream, fcntl.LOCK_UN)
+    #firstrun = True
     try:
-        with open(counterstatefile,"r") as counterstream:
-            counterheader=counterstream.readline();
-            counterline=counterstream.readline();
-            counters=[int(x) for x in counterline.rstrip("\n").split(",")];
+        with open(counterstatefile, "r") as counterstream:
+            counterheader = counterstream.readline()
+            counterline = counterstream.readline()
+            counters = [int(x) for x in counterline.rstrip("\n").split(",")]
     except IOError:
-        print "File path \""+counterstatefile+"\" does not exist.";
-        sys.stdout.flush();
-        raise;
+        print "File path \"" + counterstatefile + "\" does not exist."
+        sys.stdout.flush()
+        raise
 
     if blocking:
-        while prevcontrollerjobsrunningq(username,dependencies) and (timeleft(starttime,controllerbuffertimelimit)>0):
-            time.sleep(sleeptime);
+        while prevcontrollerjobsrunningq(username, dependencies) and (timeleft(starttime, controllerbuffertimelimit) > 0):
+            time.sleep(sleeptime)
 
-    reloadjob=(queries[0]=="RELOAD");
+    reloadjob = (queries[0] == "RELOAD")
     if reloadjob:
-        reloadstatefilename="reloadstate";
-        if len(queries)>1:
-            reloadpath=controllerpath+"/reload";
-            reloadpattern=queries[1];
-        if len(queries)>2:
-            querylimit=queries[2];
+        reloadstatefilename = "reloadstate"
+        if len(queries) > 1:
+            reloadpath = controllerpath + "/reload"
+            reloadpattern = queries[1]
+        if len(queries) > 2:
+            querylimit = queries[2]
         else:
-            querylimit=None;
+            querylimit = None
     else:
-        querystatefilename="querystate";
-        #basecollpattern=basecollection;
-        querylimitlist=[x[3]['LIMIT'] for x in queries if (len(x)>3) and ("LIMIT" in x[3].keys())];
-        if len(querylimitlist)>0:
-            querylimit=functools.reduce(operator.mul,querylimitlist,1);
+        querystatefilename = "querystate"
+        #basecollpattern = basecollection
+        querylimitlist = [x[3]['LIMIT'] for x in queries if (len(x) > 3) and ("LIMIT" in x[3].keys())]
+        if len(querylimitlist) > 0:
+            querylimit = functools.reduce(operator.mul, querylimitlist, 1)
         else:
-            querylimit=None;
+            querylimit = None
 
-    #if querylimit!=None:
-    #    niters_orig=min(niters_orig,querylimit);
+    #if querylimit != None:
+    #    niters_orig = min(niters_orig, querylimit)
     
-    firstlastrun=(not (prevcontrollerjobsrunningq(username,dependencies) or controllerjobsrunningq(username,modname,controllername)));
-    #counters[0]=1;
-    #counters[1]=1;
-    while (prevcontrollerjobsrunningq(username,dependencies) or controllerjobsrunningq(username,modname,controllername) or firstlastrun) and ((querylimit==None) or (counters[1]<=querylimit+1)) and (timeleft(starttime,controllerbuffertimelimit)>0):
-        #oldqueryresultinds=[dict([(y,x[y]) for y in dbindexes]+[(newfield,{"$exists":True})]) for x in queryresult];
-        #if len(oldqueryresultinds)==0:
-        #    oldqueryresult=[];
+    firstlastrun = (not (prevcontrollerjobsrunningq(username, dependencies) or controllerjobsrunningq(username, modname, controllername)))
+    #counters[0] = 1
+    #counters[1] = 1
+    while (prevcontrollerjobsrunningq(username, dependencies) or controllerjobsrunningq(username, modname, controllername) or firstlastrun) and ((querylimit == None) or (counters[1] <= querylimit + 1)) and (timeleft(starttime, controllerbuffertimelimit) > 0):
+        #oldqueryresultinds = [dict([(y, x[y]) for y in dbindexes] + [(newfield, {"$exists": True})]) for x in queryresult]
+        #if len(oldqueryresultinds) == 0:
+        #    oldqueryresult = []
         #else:
-        #    oldqueryresult=mongolink.collectionfind(db,newcollection,{"$or":oldqueryresultinds},dict([("_id",0)]+[(y,1) for y in dbindexes]));
-        #oldqueryresultrunning=[y for x in userjobsrunninglist(username,modname,controllername) for y in contractededjobname2jobdocs(x,dbindexes) if len(x)>0];
-        #with open(querystatefile,"a") as iostream:
-        #    for line in oldqueryresult+oldqueryresultrunning:
-        #        iostream.write(str(dict([(x,line[x]) for x in allindexes if x in line.keys()])).replace(" ","")+"\n");
-        #        iostream.flush();
-        #print "Next Run";
-        #print "hi";
-        #sys.stdout.flush();
-        #print str(starttime)+" "+str(controllerbuffertimelimit);
-        #sys.stdout.flush();
-        #if not islimitreached(controllerpath,querylimit):
+        #    oldqueryresult = mongojoin.collectionfind(db, newcollection, {"$or": oldqueryresultinds}, dict([("_id", 0)] + [(y, 1) for y in dbindexes]))
+        #oldqueryresultrunning = [y for x in userjobsrunninglist(username, modname, controllername) for y in contractededjobname2jobdocs(x, dbindexes) if len(x) > 0]
+        #with open(querystatefile, "a") as iostream:
+        #    for line in oldqueryresult + oldqueryresultrunning:
+        #        iostream.write(str(dict([(x, line[x]) for x in allindexes if x in line.keys()])).replace(" ", "") + "\n")
+        #        iostream.flush()
+        #print "Next Run"
+        #print "hi"
+        #sys.stdout.flush()
+        #print str(starttime) + " " + str(controllerbuffertimelimit)
+        #sys.stdout.flush()
+        #if not islimitreached(controllerpath, querylimit):
         if reloadjob:
-            requeueskippedreloadjobs(modname,controllername,controllerpath,reloadstatefilename,reloadpath,counters,counterstatefile,counterheader,dbindexes);
-            if (querylimit==None) or (counters[1]<=querylimit):
-                counters=reloadcrawl(reloadpath,reloadpattern,controllerpath,reloadstatefilename=reloadstatefilename,inputfunc=lambda x:doinput(x,querylimit,counters,reloadjob,storagelimit,nthreadsfield,needslicense,username,modname,controllername,controllerpath,reloadstatefilename,reloadpath,globalmaxjobcount,localmaxjobcount,localbinpath,licensescript,sublicensescript,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,partitionsmaxmemory,scriptmemorylimit,localmaxstepcount,dbindexes,niters_orig),inputdoc=doinput([],querylimit,counters,reloadjob,storagelimit,nthreadsfield,needslicense,username,modname,controllername,controllerpath,reloadstatefilename,reloadpath,globalmaxjobcount,localmaxjobcount,localbinpath,licensescript,sublicensescript,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,partitionsmaxmemory,scriptmemorylimit,localmaxstepcount,dbindexes,niters_orig),action=lambda x,y,z:doaction(x,y,z,querylimit,reloadjob,storagelimit,nthreadsfield,needslicense,username,globalmaxjobcount,localmaxjobcount,controllerpath,localbinpath,licensescript,sublicensescript,scriptlanguage,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,partitionsmaxmemory,scriptmemorylimit,localmaxstepcount,modname,controllername,dbindexes,logging,cleanup,emplocal,writelocal,writedb,statslocal,statsdb,markdone,writemode,scriptcommand,scriptflags,scriptext,scriptargs,reloadstatefilename,reloadpath,counterstatefile,counterheader,niters_orig,nbatch_orig,nworkers_orig),filereadform=lambda x:x,filewriteform=lambda x:x,docwriteform=lambda x:"_".join(indexdoc2indexsplit(x,dbindexes)),timeleft=lambda:timeleft(starttime,controllerbuffertimelimit),counters=counters,counterupdate=lambda x:docounterupdate(x,counterstatefile,counterheader),resetstatefile=False,limit=querylimit);
+            requeueskippedreloadjobs(modname, controllername, controllerpath, reloadstatefilename, reloadpath, counters, counterstatefile, counterheader, dbindexes)
+            if (querylimit == None) or (counters[1] <= querylimit):
+                counters = reloadcrawl(reloadpath, reloadpattern, controllerpath, reloadstatefilename = reloadstatefilename, inputfunc = lambda x: doinput(x, querylimit, counters, reloadjob, storagelimit, nthreadsfield, needslicense, username, modname, controllername, controllerpath, reloadstatefilename, reloadpath, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, starttime, controllerbuffertimelimit, statusstatefile, sleeptime, partitions, partitionsmaxmemory, scriptmemorylimit, localmaxstepcount, dbindexes, niters_orig), inputdoc = doinput([], querylimit, counters, reloadjob, storagelimit, nthreadsfield, needslicense, username, modname, controllername, controllerpath, reloadstatefilename, reloadpath, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, starttime, controllerbuffertimelimit, statusstatefile, sleeptime, partitions, partitionsmaxmemory, scriptmemorylimit, localmaxstepcount, dbindexes, niters_orig), action = lambda x, y,z: doaction(x, y,z, querylimit, reloadjob, storagelimit, nthreadsfield, needslicense, username, globalmaxjobcount, localmaxjobcount, controllerpath, localbinpath, licensescript, sublicensescript, scriptlanguage, starttime, controllerbuffertimelimit, statusstatefile, sleeptime, partitions, partitionsmaxmemory, scriptmemorylimit, localmaxstepcount, modname, controllername, dbindexes, logging, cleanup, emplocal, writelocal, writedb, statslocal, statsdb, markdone, writemode, scriptcommand, scriptflags, scriptext, scriptargs, reloadstatefilename, reloadpath, counterstatefile, counterheader, niters_orig, nbatch_orig, nworkers_orig), filereadform = lambda x: x, filewriteform = lambda x: x, docwriteform = lambda x: "_".join(indexdoc2indexsplit(x, dbindexes)), timeleft = lambda: timeleft(starttime, controllerbuffertimelimit), counters = counters, counterupdate = lambda x: docounterupdate(x, counterstatefile, counterheader), resetstatefile = False, limit = querylimit)
         else:
-            requeueskippedqueryjobs(modname,controllername,controllerpath,querystatefilename,basecollection,counters,counterstatefile,counterheader,dbindexes);
-            if (querylimit==None) or (counters[1]<=querylimit):
-                if dbtype=="mongodb":
-                    counters=mongolink.dbcrawl(db,queries,controllerpath,statefilename=querystatefilename,inputfunc=lambda x:doinput(x,querylimit,counters,reloadjob,storagelimit,nthreadsfield,needslicense,username,modname,controllername,controllerpath,querystatefilename,basecollection,globalmaxjobcount,localmaxjobcount,localbinpath,licensescript,sublicensescript,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,partitionsmaxmemory,scriptmemorylimit,localmaxstepcount,dbindexes,niters_orig),inputdoc=doinput([],querylimit,counters,reloadjob,storagelimit,nthreadsfield,needslicense,username,modname,controllername,controllerpath,querystatefilename,basecollection,globalmaxjobcount,localmaxjobcount,localbinpath,licensescript,sublicensescript,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,partitionsmaxmemory,scriptmemorylimit,localmaxstepcount,dbindexes,niters_orig),action=lambda x,y,z:doaction(x,y,z,querylimit,reloadjob,storagelimit,nthreadsfield,needslicense,username,globalmaxjobcount,localmaxjobcount,controllerpath,localbinpath,licensescript,sublicensescript,scriptlanguage,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,partitionsmaxmemory,scriptmemorylimit,localmaxstepcount,modname,controllername,dbindexes,logging,cleanup,templocal,writelocal,writedb,statslocal,statsdb,markdone,writemode,scriptcommand,scriptflags,scriptext,scriptargs,querystatefilename,basecollection,counterstatefile,counterheader,niters_orig,nbatch_orig,nworkers_orig),readform=lambda x:indexsplit2indexdoc(x.split("_")[2:],dbindexes),writeform=lambda x:modname+"_"+controllername+"_"+"_".join(indexdoc2indexsplit(x,dbindexes)),timeleft=lambda:timeleft(starttime,controllerbuffertimelimit),counters=counters,counterupdate=lambda x:docounterupdate(x,counterstatefile,counterheader),resetstatefile=False,limit=querylimit,limittries=10,toplevel=True);
-        #print "bye";
-        #firstrun=False;
-        releaseheldjobs(username,modname,controllername);
-        if (timeleft(starttime,controllerbuffertimelimit)>0):
-            firstlastrun=(not (prevcontrollerjobsrunningq(username,dependencies) or controllerjobsrunningq(username,modname,controllername) or firstlastrun));
-    #while controllerjobsrunningq(username,modname,controllername) and (timeleft(starttime,controllerbuffertimelimit)>0):
-    #    releaseheldjobs(username,modname,controllername);
-    #    skippedjobs=skippedjobslist(username,modname,controllername,workpath);
+            requeueskippedqueryjobs(modname, controllername, controllerpath, querystatefilename, basecollection, counters, counterstatefile, counterheader, dbindexes)
+            if (querylimit == None) or (counters[1] <= querylimit):
+                if dbtype == "mongodb":
+                    counters = mongojoin.dbcrawl(db, queries, controllerpath, statefilename = querystatefilename, inputfunc = lambda x: doinput(x, querylimit, counters, reloadjob, storagelimit, nthreadsfield, needslicense, username, modname, controllername, controllerpath, querystatefilename, basecollection, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, starttime, controllerbuffertimelimit, statusstatefile, sleeptime, partitions, partitionsmaxmemory, scriptmemorylimit, localmaxstepcount, dbindexes, niters_orig), inputdoc = doinput([], querylimit, counters, reloadjob, storagelimit, nthreadsfield, needslicense, username, modname, controllername, controllerpath, querystatefilename, basecollection, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, starttime, controllerbuffertimelimit, statusstatefile, sleeptime, partitions, partitionsmaxmemory, scriptmemorylimit, localmaxstepcount, dbindexes, niters_orig), action = lambda x, y,z: doaction(x, y,z, querylimit, reloadjob, storagelimit, nthreadsfield, needslicense, username, globalmaxjobcount, localmaxjobcount, controllerpath, localbinpath, licensescript, sublicensescript, scriptlanguage, starttime, controllerbuffertimelimit, statusstatefile, sleeptime, partitions, partitionsmaxmemory, scriptmemorylimit, localmaxstepcount, modname, controllername, dbindexes, logging, cleanup, templocal, writelocal, writedb, statslocal, statsdb, markdone, writemode, scriptcommand, scriptflags, scriptext, scriptargs, querystatefilename, basecollection, counterstatefile, counterheader, niters_orig, nbatch_orig, nworkers_orig), readform = lambda x: indexsplit2indexdoc(x.split("_")[2:], dbindexes), writeform = lambda x: modname + "_" + controllername + "_" + "_".join(indexdoc2indexsplit(x, dbindexes)), timeleft = lambda: timeleft(starttime, controllerbuffertimelimit), counters = counters, counterupdate = lambda x: docounterupdate(x, counterstatefile, counterheader), resetstatefile = False, limit = querylimit, limittries = 10, toplevel = True)
+        #print "bye"
+        #firstrun = False
+        releaseheldjobs(username, modname, controllername)
+        if (timeleft(starttime, controllerbuffertimelimit) > 0):
+            firstlastrun = (not (prevcontrollerjobsrunningq(username, dependencies) or controllerjobsrunningq(username, modname, controllername) or firstlastrun))
+    #while controllerjobsrunningq(username, modname, controllername) and (timeleft(starttime, controllerbuffertimelimit) > 0):
+    #    releaseheldjobs(username, modname, controllername)
+    #    skippedjobs = skippedjobslist(username, modname, controllername, workpath)
     #    for x in skippedjobs:
-    #        maxmemorypernode=getmaxmemorypernode(resourcesstatefile,controllerpartition);
-    #        submitjob(workpath,x,controllerpartition,maxmemorypernode,maxmemorypernode,resubmit=True);
-    #    time.sleep(sleeptime);
+    #        maxmemorypernode = getmaxmemorypernode(resourcesstatefile, controllerpartition)
+    #        submitjob(workpath, x,controllerpartition, maxmemorypernode, maxmemorypernode, resubmit = True)
+    #    time.sleep(sleeptime)
 
-    if (prevcontrollerjobsrunningq(username,dependencies) or controllerjobsrunningq(username,modname,controllername) or firstlastrun) and not (timeleft(starttime,controllerbuffertimelimit)>0):
+    if (prevcontrollerjobsrunningq(username, dependencies) or controllerjobsrunningq(username, modname, controllername) or firstlastrun) and not (timeleft(starttime, controllerbuffertimelimit) > 0):
         #Resubmit controller job
-        #maxmemorypernode=getmaxmemorypernode(resourcesstatefile,controllerpartition);
-        assert isinstance(configdoc["resources"][controllerpartition],int);
-        maxmemorypernode=configdoc["resources"][controllerpartition];
+        #maxmemorypernode = getmaxmemorypernode(resourcesstatefile, controllerpartition)
+        assert isinstance(configdoc["resources"][controllerpartition], int)
+        maxmemorypernode = configdoc["resources"][controllerpartition]
 
-        loadpathnames=glob.iglob(workpath+"/*.docs");
-        with open(controllerpath+"/skipped","a") as skippedstream:
+        loadpathnames = glob.iglob(workpath + "/*.docs")
+        with open(controllerpath + "/skipped", "a") as skippedstream:
             for loadpathname in loadpathnames:
-                loadfilename=loadpathname.split("/")[-1];
-                errloadpathname=loadpathname.replace(".docs",".err");
-                errcode="-1:0"
+                loadfilename = loadpathname.split("/")[-1]
+                errloadpathname = loadpathname.replace(".docs", ".err")
+                errcode = "-1:0"
                 if os.path.exists(errloadpathname):
-                    with open(errloadpathname,"r") as errstream:
+                    with open(errloadpathname, "r") as errstream:
                         for errline in errstream:
                             if "ExitCode: " in errline:
-                                errcode=errline.rstrip("\n").replace("ExitCode: ","");
-                                break;
-                skippedstream.write(loadfilename+","+errcode+",True\n");
-                skippedstream.flush();
+                                errcode = errline.rstrip("\n").replace("ExitCode: ", "")
+                                break
+                skippedstream.write(loadfilename + ", " + errcode + ", True\n")
+                skippedstream.flush()
 
-        submitcontrollerjob(controllerpath,"crunch_"+modname+"_"+controllername+"_controller",controllernnodes,controllerncores,controllerpartition,maxmemorypernode,resubmit=True);
-        with open(statusstatefile,"w") as statusstream:
-            statusstream.truncate(0);
-            statusstream.write("Resubmitting");
+        submitcontrollerjob(controllerpath, "crunch_" + modname + "_" + controllername + "_controller", controllernnodes, controllerncores, controllerpartition, maxmemorypernode, resubmit = True)
+        with open(statusstatefile, "w") as statusstream:
+            statusstream.truncate(0)
+            statusstream.write("Resubmitting")
 
     else:
-        #if pdffile!="":
-        #    plotjobgraph(modname,controllerpath,controllername,workpath,pdffile);
-        with open(statusstatefile,"w") as statusstream:
-            statusstream.truncate(0);
-            statusstream.write("Completing");
-        print "";
-        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
-        print "Completing job crunch_"+modname+"_"+controllername+"_controller\n";
-        sys.stdout.flush();
-
-    #querystream.close();
-    #seekstream.close();
+        #if pdffile != "":
+        #    plotjobgraph(modname, controllerpath, controllername, workpath, pdffile)
+        with open(statusstatefile, "w") as statusstream:
+            statusstream.truncate(0)
+            statusstream.write("Completing")
+        print ""
+        print datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")
+        print "Completing job crunch_" + modname + "_" + controllername + "_controller\n"
+        sys.stdout.flush()
+
+    #querystream.close()
+    #seekstream.close()
     #if needslicense:
-        #fcntl.flock(pendlicensestream,fcntl.LOCK_UN);
-    #    licensestream.close();
-    if dbtype=="mongodb":
-        dbclient.close();
+        #fcntl.flock(pendlicensestream, fcntl.LOCK_UN)
+    #    licensestream.close()
+    if dbtype == "mongodb":
+        dbclient.close()
 except Exception as e:
-    PrintException();
\ No newline at end of file
+    PrintException()
\ No newline at end of file
diff --git a/bin/crunch b/bin/crunch
index 5a7b98f..45b7b4c 100755
--- a/bin/crunch
+++ b/bin/crunch
@@ -1,73 +1,242 @@
 #!/bin/bash
 
-do_crunch () {
-    modpath=$1
-    controllerpath=$2
-    workpath=$3
+do_create() {
+    is_create=$1
+    path=$2
 
-    jobs=$(find ${controllerpath} -path '*/.*' -prune -o -type f -name "*.job" -print 2>/dev/null)
-    for job in ${jobs}
+    if ${is_create}
+    then
+        mkdir ${path}
+    fi
+}
+
+run_subcommand() {
+    scriptpath=$1
+    shift
+
+    is_create=$1
+    shift
+
+    modules_dir=$1
+    shift
+
+    startpath=$(cd ${modules_dir} && pwd -P)
+
+    currdir=${startpath}
+
+    module_paths=($(find ${currdir} -mindepth 1 -type d -path '*/.*' -prune -o -type f -name '.DBCrunch' -execdir pwd -P \;))
+
+    currdir=$(cd ${currdir}/.. && pwd -P)
+    dirstats=$(stat -c '%a %U' ${currdir})
+    while [[ "${dirstats}" =~ ^"777 " ]] || [[ "${dirstats}" =~ " ${USER}"$ ]]
     do
-        jobname=$(echo ${job} | rev | cut -d'/' -f1 | rev)
-        jobmsg=$(sbatch ${job} 2>&1)
-        echo "${jobmsg} as ${jobname}."
+        if [ -f "${currdir}/.DBCrunch" ]
+        then
+            module_paths=("${currdir}" "${module_paths[@]}")
+        fi
+        currdir=$(cd ${currdir}/.. && pwd -P)
+        dirstats=$(stat -c '%a %U' ${currdir})
     done
-}
 
-do_not_dir() {
-    path=$1
+    if [ "${#module_paths[@]}" -eq 0 ]
+    then
+        echo "Error: No module path detected."
+        exit 1
+    elif [ "${#module_paths[@]}" -gt 1 ]
+    then
+        echo "Error: Multiple module paths detected. Please choose one:"
+        for i in ${!module_paths[@]}
+        do
+            echo "  ${i}) ${module_paths[${i}]}"
+        done
+        exit 1
+    fi
+
+    moddirpath=${module_paths[0]}
+    pathdiff=${startpath##${moddirpath}/}
+    if [[ "${pathdiff}" == "${startpath}" ]]
+    then
+        pathdiff=""
+    fi
+    read modname controllername <<< $(echo ${pathdiff} | sed 's/\// /g' | cut -d' ' -f1,2)
+
+    case $# in
+        0)
+            if [[ "${modname}" != "" ]]
+            then
+                if [[ "${controllername}" != "" ]] && [ -d "${moddirpath}/${modname}/${controllername}" ]
+                then
+                    modnames=(${modname})
+                    controllernames=(${controllername})
+                else
+                    modnames=(${modname})
+                    controllernames=($(find ${moddirpath}/${modname} -mindepth 1 -maxdepth 1 -type d -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev))
+                fi
+            else
+                modnames=($(find ${moddirpath} -mindepth 1 -maxdepth 1 -type d -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev))
+                controllernames=()
+            fi
+            #modnames=(${modname})
+            #controllernames=(${controllername})
+            ;;
+        1)
+            case $1 in
+                ${modname} | ${controllername})
+                    modnames=(${modname})
+                    controllernames=(${controllername})
+                    ;;
+                *)
+                    if [[ "${modname}" != "" ]]
+                    then
+                        if [ -d "${moddirpath}/${modname}/$1" ]
+                        then
+                            modnames=(${modname})
+                            controllernames=($1)
+                        else
+                            if ${is_create}
+                            then
+                                modnames=(${modname})
+                                controllernames=($1)
+                            else
+                                modnames=($1)
+                                controllernames=()
+                            fi
+                        fi
+                    else
+                        modnames=($1)
+                        controllernames=()
+                    fi
+                    ;;
+            esac
+            ;;
+        2)
+            modnames=($1)
+            controllernames=($2)
+            ;;
+        *)
+            echo "Error: You must provide no more than 2 arguments. $# provided."
+            exit 1
+            ;;
+    esac
+
+    for i in ${!modnames[@]}
+    do
+        modname=${modnames[${i}]}
+
+        modpath="${moddirpath}/${modname}"
+
+        if [ ! -d "${modpath}" ] && ${is_create}
+        then
+            do_create ${is_create} ${modpath}
+        fi
+
+        if [[ "${controllernames[${i}]}" == "" ]]
+        then
+            controllernames=($(find ${modpath} -mindepth 1 -maxdepth 1 -type d -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev))
+
+            for controllername in ${controllernames[@]}
+            do
+                controllerpath="${modpath}/${controllername}"
+
+                if [ ! -d "${controllerpath}" ] && ${is_create}
+                then
+                    do_create ${is_create} ${controllerpath}
+                fi
+
+                workpath="${controllerpath}/jobs"
+
+                if [ ! -d "${workpath}" ] && ${is_create}
+                then
+                    do_create ${is_create} ${workpath}
+                fi
+
+                ${scriptpath} ${modpath} ${controllerpath} ${workpath}
+            done
+        else
+            controllername=${controllernames[${i}]}
+            controllerpath="${modpath}/${controllername}"
+
+            if [ ! -d "${controllerpath}" ] && ${is_create}
+            then
+                do_create ${is_create} ${controllerpath}
+            fi
+
+            workpath="${controllerpath}/jobs"
+
+            if [ ! -d "${workpath}" ] && ${is_create}
+            then
+                do_create ${is_create} ${workpath}
+            fi
+
+            ${scriptpath} ${modpath} ${controllerpath} ${workpath}
+        fi
+    done
 }
 
 ProgName=$(basename "${BASH_SOURCE[0]}")
 ProgDir=$(cd $(dirname "${BASH_SOURCE[0]}"); pwd -P)
   
 sub_help(){
-    echo "Usage: $ProgName [<subcommand>] [options]\n"
-    echo "    crunch            Submit controller batch job in current directory"
-    echo "Subcommands:"
-    echo "    crunch cancel     Cancel module or controller jobs"
-    echo "    crunch monitor    Monitor all controller jobs"
-    echo "    crunch requeue    Requeue jobs skipped by controller"
-    echo "    crunch reset      Reset controller for a module"
-    echo "    crunch submit     Submit controller batch job"
-    echo "    crunch template   Create new controller for a module from a template"
+    echo "Usage: $ProgName [options] [<subcommand>] [module_name/controller_name] [controller_name]\n"
     echo ""
-    echo "For help with each subcommand run:"
-    echo "$ProgName <subcommand> -h|--help"
+    echo "Options:"
+    echo "    crunch --modules-dir [<dir>]     Specify a working directory other than the current one"
     echo ""
+    echo "Subcommands:"
+    echo "    crunch cancel                    Cancel module or controller jobs"
+    echo "    crunch monitor                   Monitor all controller jobs"
+    echo "    crunch requeue                   Requeue jobs skipped by controller"
+    echo "    crunch reset                     Reset controller for a module"
+    echo "    crunch submit                    Submit controller batch job"
+    echo "    crunch template                  Create new controller for a module from a template"
+    echo "    crunch test (true/false)         Test what modules and controllers are matched"
+    echo ""
+    #echo "For help with each subcommand run:"
+    #echo "$ProgName <subcommand> -h|--help"
+    #echo ""
 }
 
 sub_cancel(){
-    ${ProgDir}/cancel ${@}
+    is_create=false
+    run_subcommand ${ProgDir}/cancel ${is_create} ${@}
 }
 
 sub_monitor(){
-    if [ "$#" -eq 0 ]
+    if [ "$#" -eq 1 ]
     then
         ${ProgDir}/monitor 1
     else
-        ${ProgDir}/monitor $1
+        ${ProgDir}/monitor $2
     fi
 }
 
 sub_requeue(){
-    ${ProgDir}/requeue ${@}
+    is_create=false
+    run_subcommand ${ProgDir}/requeue ${is_create} ${@}
 }
 
 sub_reset(){
-    ${ProgDir}/reset ${@}
+    is_create=false
+    run_subcommand ${ProgDir}/reset ${is_create} ${@}
 }
 
 sub_submit(){
-    ${ProgDir}/submit ${@}
+    is_create=false
+    run_subcommand ${ProgDir}/submit ${is_create} ${@}
 }
 
 sub_template(){
-    ${ProgDir}/template ${@}
+    is_create=true
+    run_subcommand ${ProgDir}/template ${is_create} ${@}
+}
+
+sub_test(){
+    is_create=$2
+    run_subcommand ${ProgDir}/test ${is_create} $1 ${@:3}
 }
 
 SHORTOPTS="m:" 
-LONGOPTS="modules_dir:" 
+LONGOPTS="modules-dir:" 
 
 ARGS=$(getopt -s bash --options $SHORTOPTS --longoptions $LONGOPTS -- "$@" ) 
 
@@ -78,7 +247,7 @@ modules_dir=$(pwd -P)
 while true
 do
     case $1 in
-        -m | --modules_dir)
+        -m | --modules-dir)
             modules_dir=$2
             shift
             ;;
@@ -97,7 +266,7 @@ done
 if [ ! -d "${modules_dir}" ]
 then
     echo "Error: Directory \"${modules_dir}\" does not exist."
-    exit
+    exit 1
 fi
   
 subcommand=$1
@@ -105,143 +274,9 @@ case $subcommand in
     "-h" | "--help")
         sub_help
         ;;
-    "")
-        startpath=${modules_dir}
-        branches=$(find ${startpath} -path '*/.*' -prune -o -type d -exec bash -c 'echo "$(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g" | grep -o / | wc -l) $(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g")"' \; | grep -v '/jobs/' | sort -u | tr '\n' ',')
-        maxdepth=$(echo ${branches} | tr ',' '\n' | cut -d' ' -f1 | sort -u | tail -n1)
-        leaves=$(echo ${branches} | tr ',' '\n' | grep "^${maxdepth} " | cut -d' ' -f2 | sed 's/\/jobs$//g' | tr '\n' ',')
-        moddirpath=$(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1,2 --complement | rev | head -n1)
-        modnames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f2 | rev))
-        controllernames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1 | rev))
-
-        case $# in
-            0)
-                ;;
-            1)
-                if [[ "${moddirpath}" == "${startpath}" ]]
-                then
-                    modnames=($1)
-                    controllernames=()
-                else
-                    stopflag=false
-                    for i in ${!controllernames[@]}
-                    do
-                        if [[ "${controllernames[${i}]}" == "$1" ]]
-                        then
-                            modnames=(${modnames[${i}]})
-                            controllernames=(${controllernames[i]})
-                            stopflag=true
-                            break
-                        fi
-                    done
-                    if ! ${stopflag}
-                    then
-                        for modname in ${modnames[@]}
-                        do
-                            if [[ "${modname}" == "$1" ]]
-                            then
-                                modnames=(${modname})
-                                controllernames=()
-                                stopflag=true
-                                break
-                            fi
-                        done
-                    fi
-                    if ! ${stopflag}
-                    then
-                        for modname in $(find ${moddirpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev)
-                        do
-                            if [[ "${modname}" == "$1" ]]
-                            then
-                                modnames=(${modname})
-                                controllernames=()
-                                stopflag=true
-                                break
-                            fi
-                        done
-                    fi
-                    if ! ${stopflag}
-                    then
-                        for i in ${!modnames[@]}
-                        do
-                            modnames=(${modnames[${i}]})
-                            controllernames=($1)
-                        done
-                    fi
-                fi
-                ;;
-            2)
-                modnames=($1)
-                controllernames=($2)
-                ;;
-            *)
-                echo "Error: You must provide no more than 2 arguments. $# provided."
-                exit
-                ;;
-        esac
-
-        for i in ${!modnames[@]}
-        do
-            modname=${modnames[${i}]}
-
-            modpath="${moddirpath}/${modname}"
-
-            if [ ! -d "${modpath}" ]
-            then
-                do_not_dir ${modpath}
-            fi
-
-            if [[ "${controllernames[${i}]}" == "" ]]
-            then
-                controllernames=($(find ${modpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev))
-
-                for controllername in ${controllernames[@]}
-                do
-                    controllerpath="${modpath}/${controllername}"
-
-                    if [ ! -d "${controllerpath}" ]
-                    then
-                        do_not_dir ${controllerpath}
-                    fi
-
-                    workpath="${controllerpath}/jobs"
-
-                    if [ ! -d "${workpath}" ]
-                    then
-                        do_not_dir ${workpath}
-                    fi
-
-                    if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-                    then
-                        do_test ${modpath} ${controllerpath} ${workpath}
-                    fi
-                done
-            else
-                controllername=${controllernames[${i}]}
-                controllerpath="${modpath}/${controllername}"
-
-                if [ ! -d "${controllerpath}" ]
-                then
-                    do_not_dir ${controllerpath}
-                fi
-
-                workpath="${controllerpath}/jobs"
-
-                if [ ! -d "${workpath}" ]
-                then
-                    do_not_dir ${workpath}
-                fi
-
-                if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-                then
-                    do_test ${modpath} ${controllerpath} ${workpath}
-                fi
-            fi
-        done
-        ;;
     *)
         shift
-        sub_${subcommand} ${modules_dir} $@
+        sub_${subcommand} ${modules_dir} ${@}
         if [ $? = 127 ]; then
             echo "Error: '$subcommand' is not a known subcommand." >&2
             echo "       Run '$ProgName --help' for a list of known subcommands." >&2
diff --git a/bin/crunch_test b/bin/crunch_test
deleted file mode 100755
index e593438..0000000
--- a/bin/crunch_test
+++ /dev/null
@@ -1,184 +0,0 @@
-#!/bin/bash
-
-do_test () {
-    modpath=$1
-    controllerpath=$2
-    workpath=$3
-
-    echo ""
-    echo ${modpath}
-    echo ${controllerpath}
-    echo ${workpath}
-}
-
-do_not_dir() {
-    path=$1
-}
-
-SHORTOPTS="m:" 
-LONGOPTS="modules_dir:" 
-
-ARGS=$(getopt -s bash --options $SHORTOPTS --longoptions $LONGOPTS -- "$@" ) 
-
-eval set -- "$ARGS" 
-
-modules_dir=$(pwd -P)
-
-while true
-do
-    case $1 in
-        -n | --modules_dir)
-            modules_dir=$2
-            shift
-            ;;
-        --)
-            shift
-            break
-            ;;
-        *)
-            shift
-            break
-            ;;
-    esac
-    shift
-done
-
-if [ ! -d "${modules_dir}" ]
-then
-    echo "Error: Directory \"$1\" does not exist."
-    exit
-fi
-
-startpath=$(cd ${modules_dir} && pwd -P)
-
-branches=$(find ${startpath} -path '*/.*' -prune -o -type d -exec bash -c 'echo "$(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g" | grep -o / | wc -l) $(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g")"' \; | grep -v '/jobs/' | sort -u | tr '\n' ',')
-maxdepth=$(echo ${branches} | tr ',' '\n' | cut -d' ' -f1 | sort -u | tail -n1)
-leaves=$(echo ${branches} | tr ',' '\n' | grep "^${maxdepth} " | cut -d' ' -f2 | sed 's/\/jobs$//g' | tr '\n' ',')
-moddirpath=$(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1,2 --complement | rev | head -n1)
-modnames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f2 | rev))
-controllernames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1 | rev))
-
-case $# in
-    0)
-        ;;
-    1)
-        if [[ "${moddirpath}" == "${startpath}" ]]
-        then
-            modnames=($1)
-            controllernames=()
-        else
-            stopflag=false
-            for i in ${!controllernames[@]}
-            do
-                if [[ "${controllernames[${i}]}" == "$1" ]]
-                then
-                    modnames=(${modnames[${i}]})
-                    controllernames=(${controllernames[i]})
-                    stopflag=true
-                    break
-                fi
-            done
-            if ! ${stopflag}
-            then
-                for modname in ${modnames[@]}
-                do
-                    if [[ "${modname}" == "$1" ]]
-                    then
-                        modnames=(${modname})
-                        controllernames=()
-                        stopflag=true
-                        break
-                    fi
-                done
-            fi
-            if ! ${stopflag}
-            then
-                for modname in $(find ${moddirpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev)
-                do
-                    if [[ "${modname}" == "$1" ]]
-                    then
-                        modnames=(${modname})
-                        controllernames=()
-                        stopflag=true
-                        break
-                    fi
-                done
-            fi
-            if ! ${stopflag}
-            then
-                for i in ${!modnames[@]}
-                do
-                    modnames=(${modnames[${i}]})
-                    controllernames=($1)
-                done
-            fi
-        fi
-        ;;
-    2)
-        modnames=($1)
-        controllernames=($2)
-        ;;
-    *)
-        echo "Error: You must provide no more than 2 arguments. $# provided."
-        exit
-        ;;
-esac
-
-for i in ${!modnames[@]}
-do
-    modname=${modnames[${i}]}
-
-    modpath="${moddirpath}/${modname}"
-
-    if [ ! -d "${modpath}" ]
-    then
-        do_not_dir ${modpath}
-    fi
-
-    if [[ "${controllernames[${i}]}" == "" ]]
-    then
-        controllernames=($(find ${modpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev))
-
-        for controllername in ${controllernames[@]}
-        do
-            controllerpath="${modpath}/${controllername}"
-
-            if [ ! -d "${controllerpath}" ]
-            then
-                do_not_dir ${controllerpath}
-            fi
-
-            workpath="${controllerpath}/jobs"
-
-            if [ ! -d "${workpath}" ]
-            then
-                do_not_dir ${workpath}
-            fi
-
-            if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-            then
-                do_test ${modpath} ${controllerpath} ${workpath}
-            fi
-        done
-    else
-        controllername=${controllernames[${i}]}
-        controllerpath="${modpath}/${controllername}"
-
-        if [ ! -d "${controllerpath}" ]
-        then
-            do_not_dir ${controllerpath}
-        fi
-
-        workpath="${controllerpath}/jobs"
-
-        if [ ! -d "${workpath}" ]
-        then
-            do_not_dir ${workpath}
-        fi
-
-        if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-        then
-            do_test ${modpath} ${controllerpath} ${workpath}
-        fi
-    fi
-done
\ No newline at end of file
diff --git a/bin/monitor b/bin/monitor
index e1b624d..f9fab3d 100755
--- a/bin/monitor
+++ b/bin/monitor
@@ -21,6 +21,15 @@ monitorjobs() {
             outsperc=$(echo "scale=2;(100*${outs})/${reads}" | bc)
             echo "${outsperc}% Finished: ${outs} of ${reads}"
         fi
+        controllertime=$(head -n1 ${controllerpath}/crunch_${modname}_${controllername}_controller.out 2>/dev/null | tr ' ' '/' | sed 's/\/\([0-9]*:\)/ \1/g' | sed 's/\/UTC/ UTC/g')
+        if [[ "${controllertime}" == "" ]]
+        then
+            echo "Duration: $(date -u -d "${controllertime}" +"%H:%M:%S")"
+        else
+            startdate=$(date -u -d "${controllertime}" "+%s")
+            enddate=$(date -u "+%s")
+            echo "Duration: $(date -u -d @$((${enddate}-${startdate})) +"%H:%M:%S")"
+        fi
         echo "Size: $(du -ch ${workpath} 2>/dev/null | tail -n1 | sed 's/\s\s*/ /g' | cut -d' ' -f1)"
         controllerjobs=$(echo ${jobs} | tr '!' '\n' 2>/dev/null | sed 's/^\s*//g' | grep "crunch_${modname}_${controllername}_job_" | head -c -1 | tr '\n' '!')
         ncontrollerjobs=$(echo ${controllerjobs} | tr '!' '\n' 2>/dev/null | grep '_steps_' | wc -l)
diff --git a/bin/requeue b/bin/requeue
index c94e141..4357102 100755
--- a/bin/requeue
+++ b/bin/requeue
@@ -1,175 +1,35 @@
 #!/bin/bash
 
-do_requeue () {
-    modpath=$1
-    controllerpath=$2
-    workpath=$3
-
-    #loadfilenames=$(find ${mainpath}/jobs/ -maxdepth 1 -type f -name "*.docs" | rev | cut -d'/' -f1 | rev | tr '\n' ',')
-    loadfilenames=$(find ${workpath} -maxdepth 1 -path '*/.*' -prune -o -type f -name "*.docs" -print 2>/dev/null | rev | cut -d'/' -f1 | rev)
-    #loadfilejobpatt=$(echo ${loadfilenames} | tr ',' '\n' | sed 's/\(.*_job_[0-9]*\).*/\1/g' | sort -u)
-    #mkdir ${mainpath}/jobs/requeued 2>/dev/null
-    #for patt in ${loadfilejobpatt}
-    #do
-    #    mv ${mainpath}/jobs/${patt}* ${mainpath}/jobs/requeued/ 2>/dev/null
-    #done
-    #for loadfilename in $(echo ${loadfilenames} | tr ',' '\n')
-    for loadfilename in ${loadfilenames}
-    do
-        if [ -s "${loadfilename/.docs/.err}" ]
-        then
-            #errcode=$(cat ${mainpath}/jobs/requeued/${loadfilename/.docs/.out} | grep "ExitCode: " | cut -d' ' -f2)
-            errcode=$(cat ${workpath}/${loadfilename/.docs/.err} | grep "ExitCode: " | cut -d' ' -f2)
-            if [[ "${errcode}" == "" ]]
-            then
-                errcode="-1:0"
-            fi
-        else
-            errcode="-1:0"
-        fi
-        echo "${loadfilename},${errcode},True"
-    done >> ${controllerpath}/skipped
-
-    echo "Finished requeuing ${modname}_${controllername}."
-}
-
-do_not_dir() {
-    path=$1
-}
-
-modules_dir=$1
-shift
-
-startpath=$(cd ${modules_dir} && pwd -P)
-
-branches=$(find ${startpath} -path '*/.*' -prune -o -type d -exec bash -c 'echo "$(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g" | grep -o / | wc -l) $(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g")"' \; | grep -v '/jobs/' | sort -u | tr '\n' ',')
-maxdepth=$(echo ${branches} | tr ',' '\n' | cut -d' ' -f1 | sort -u | tail -n1)
-leaves=$(echo ${branches} | tr ',' '\n' | grep "^${maxdepth} " | cut -d' ' -f2 | sed 's/\/jobs$//g' | tr '\n' ',')
-moddirpath=$(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1,2 --complement | rev | head -n1)
-modnames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f2 | rev))
-controllernames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1 | rev))
-
-case $# in
-    0)
-        ;;
-    1)
-        if [[ "${moddirpath}" == "${startpath}" ]]
-        then
-            modnames=($1)
-            controllernames=()
-        else
-            stopflag=false
-            for i in ${!controllernames[@]}
-            do
-                if [[ "${controllernames[${i}]}" == "$1" ]]
-                then
-                    modnames=(${modnames[${i}]})
-                    controllernames=(${controllernames[i]})
-                    stopflag=true
-                    break
-                fi
-            done
-            if ! ${stopflag}
-            then
-                for modname in ${modnames[@]}
-                do
-                    if [[ "${modname}" == "$1" ]]
-                    then
-                        modnames=(${modname})
-                        controllernames=()
-                        stopflag=true
-                        break
-                    fi
-                done
-            fi
-            if ! ${stopflag}
-            then
-                for modname in $(find ${moddirpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev)
-                do
-                    if [[ "${modname}" == "$1" ]]
-                    then
-                        modnames=(${modname})
-                        controllernames=()
-                        stopflag=true
-                        break
-                    fi
-                done
-            fi
-            if ! ${stopflag}
-            then
-                for i in ${!modnames[@]}
-                do
-                    modnames=(${modnames[${i}]})
-                    controllernames=($1)
-                done
-            fi
-        fi
-        ;;
-    2)
-        modnames=($1)
-        controllernames=($2)
-        ;;
-    *)
-        echo "Error: You must provide no more than 2 arguments. $# provided."
-        exit
-        ;;
-esac
-
-for i in ${!modnames[@]}
+modpath=$1
+controllerpath=$2
+workpath=$3
+
+modname=$(echo ${modpath} | rev | cut -d'/' -f1 | rev)
+controllername=$(echo ${controllerpath} | rev | cut -d'/' -f1 | rev)
+
+#loadfilenames=$(find ${mainpath}/jobs/ -maxdepth 1 -type f -name "*.docs" | rev | cut -d'/' -f1 | rev | tr '\n' ',')
+loadfilenames=$(find ${workpath} -maxdepth 1 -type d -path '*/.*' -prune -o -type f -name "*.docs" -print 2>/dev/null | rev | cut -d'/' -f1 | rev)
+#loadfilejobpatt=$(echo ${loadfilenames} | tr ',' '\n' | sed 's/\(.*_job_[0-9]*\).*/\1/g' | sort -u)
+#mkdir ${mainpath}/jobs/requeued 2>/dev/null
+#for patt in ${loadfilejobpatt}
+#do
+#    mv ${mainpath}/jobs/${patt}* ${mainpath}/jobs/requeued/ 2>/dev/null
+#done
+#for loadfilename in $(echo ${loadfilenames} | tr ',' '\n')
+for loadfilename in ${loadfilenames}
 do
-    modname=${modnames[${i}]}
-
-    modpath="${moddirpath}/${modname}"
-
-    if [ ! -d "${modpath}" ]
-    then
-        do_not_dir ${modpath}
-    fi
-
-    if [[ "${controllernames[${i}]}" == "" ]]
+    if [ -s "${loadfilename/.docs/.err}" ]
     then
-        controllernames=($(find ${modpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev))
-
-        for controllername in ${controllernames[@]}
-        do
-            controllerpath="${modpath}/${controllername}"
-
-            if [ ! -d "${controllerpath}" ]
-            then
-                do_not_dir ${controllerpath}
-            fi
-
-            workpath="${controllerpath}/jobs"
-
-            if [ ! -d "${workpath}" ]
-            then
-                do_not_dir ${workpath}
-            fi
-
-            if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-            then
-                do_requeue ${modpath} ${controllerpath} ${workpath}
-            fi
-        done
-    else
-        controllername=${controllernames[${i}]}
-        controllerpath="${modpath}/${controllername}"
-
-        if [ ! -d "${controllerpath}" ]
+        #errcode=$(cat ${mainpath}/jobs/requeued/${loadfilename/.docs/.out} | grep "ExitCode: " | cut -d' ' -f2)
+        errcode=$(cat ${workpath}/${loadfilename/.docs/.err} | grep "ExitCode: " | cut -d' ' -f2)
+        if [[ "${errcode}" == "" ]]
         then
-            do_not_dir ${controllerpath}
-        fi
-
-        workpath="${controllerpath}/jobs"
-
-        if [ ! -d "${workpath}" ]
-        then
-            do_not_dir ${workpath}
-        fi
-
-        if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-        then
-            do_requeue ${modpath} ${controllerpath} ${workpath}
+            errcode="-1:0"
         fi
+    else
+        errcode="-1:0"
     fi
-done
\ No newline at end of file
+    echo "${loadfilename},${errcode},True"
+done >> ${controllerpath}/skipped
+
+echo "Finished requeuing ${modname}_${controllername}."
\ No newline at end of file
diff --git a/bin/reset b/bin/reset
index 2488255..ee510cb 100755
--- a/bin/reset
+++ b/bin/reset
@@ -1,176 +1,36 @@
 #!/bin/bash
 
-do_reset () {
-    modpath=$1
-    controllerpath=$2
-    workpath=$3
-
-    #workpath="${controllerpath}/jobs"
-    #mongolinkpath="${SLURMONGO_ROOT}/packages/python/mongolink"
-    #toolspath="${SLURMONGO_ROOT}/scripts/tools"
-
-    rm ${controllerpath}/*.out ${controllerpath}/*.err tmp* 2>/dev/null
-    rm -r ${workpath}/* 2>/dev/null
-    rm ${controllerpath}/querystate* 2>/dev/null
-    rm ${controllerpath}/reloadstate* 2>/dev/null
-
-    echo "JobStep,ExitCode,Resubmit?" > ${controllerpath}/skipped 2>/dev/null
-    echo -e "BatchCounter,StepCounter\n1,1" > ${controllerpath}/batchcounter 2>/dev/null
-    echo "Pending" > ${controllerpath}/status 2>/dev/null
-
-    echo "Finished resetting ${modname}_${controllername}."
-
-    #currdir=$(pwd)
-    #cd ${mongolinkpath}
-    #python setup.py install --user --record filespy.txt
-    #sage --python setup.py install --user --record filessage.txt
-    #cd ${currdir}
-
-    #mongouri=$(cat ${controllerpath}/controller*.job | grep "mongouri=" | cut -d'=' -f2 | sed 's/"//g')
-    #basecollection=$(cat ${controllerpath}/controller*.job | grep "basecollection=" | cut -d'=' -f2 | sed 's/"//g')
-    #modname=$(cat ${controllerpath}/controller*.job | grep "modname=" | cut -d'=' -f2 | sed 's/"//g')
-    #markdone=$(cat ${controllerpath}/controller*.job | grep "markdone=" | cut -d'=' -f2 | sed 's/"//g')
-    #h11=$(cat ${controllerpath}/controller*.job | grep "h11=" | cut -d'=' -f2 | sed 's/"//g')
-    #python ${toolspath}/unmark.py "${basecollection}" "${modname}" "${markdone}" "{\"H11\":${h11}}"
-}
-
-do_not_dir() {
-    path=$1
-}
-
-modules_dir=$1
-shift
-
-startpath=$(cd ${modules_dir} && pwd -P)
-
-branches=$(find ${startpath} -path '*/.*' -prune -o -type d -exec bash -c 'echo "$(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g" | grep -o / | wc -l) $(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g")"' \; | grep -v '/jobs/' | sort -u | tr '\n' ',')
-maxdepth=$(echo ${branches} | tr ',' '\n' | cut -d' ' -f1 | sort -u | tail -n1)
-leaves=$(echo ${branches} | tr ',' '\n' | grep "^${maxdepth} " | cut -d' ' -f2 | sed 's/\/jobs$//g' | tr '\n' ',')
-moddirpath=$(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1,2 --complement | rev | head -n1)
-modnames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f2 | rev))
-controllernames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1 | rev))
-
-case $# in
-    0)
-        ;;
-    1)
-        if [[ "${moddirpath}" == "${startpath}" ]]
-        then
-            modnames=($1)
-            controllernames=()
-        else
-            stopflag=false
-            for i in ${!controllernames[@]}
-            do
-                if [[ "${controllernames[${i}]}" == "$1" ]]
-                then
-                    modnames=(${modnames[${i}]})
-                    controllernames=(${controllernames[i]})
-                    stopflag=true
-                    break
-                fi
-            done
-            if ! ${stopflag}
-            then
-                for modname in ${modnames[@]}
-                do
-                    if [[ "${modname}" == "$1" ]]
-                    then
-                        modnames=(${modname})
-                        controllernames=()
-                        stopflag=true
-                        break
-                    fi
-                done
-            fi
-            if ! ${stopflag}
-            then
-                for modname in $(find ${moddirpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev)
-                do
-                    if [[ "${modname}" == "$1" ]]
-                    then
-                        modnames=(${modname})
-                        controllernames=()
-                        stopflag=true
-                        break
-                    fi
-                done
-            fi
-            if ! ${stopflag}
-            then
-                for i in ${!modnames[@]}
-                do
-                    modnames=(${modnames[${i}]})
-                    controllernames=($1)
-                done
-            fi
-        fi
-        ;;
-    2)
-        modnames=($1)
-        controllernames=($2)
-        ;;
-    *)
-        echo "Error: You must provide no more than 2 arguments. $# provided."
-        exit
-        ;;
-esac
-
-for i in ${!modnames[@]}
-do
-    modname=${modnames[${i}]}
-
-    modpath="${moddirpath}/${modname}"
-
-    if [ ! -d "${modpath}" ]
-    then
-        do_not_dir ${modpath}
-    fi
-
-    if [[ "${controllernames[${i}]}" == "" ]]
-    then
-        controllernames=($(find ${modpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev))
-
-        for controllername in ${controllernames[@]}
-        do
-            controllerpath="${modpath}/${controllername}"
-
-            if [ ! -d "${controllerpath}" ]
-            then
-                do_not_dir ${controllerpath}
-            fi
-
-            workpath="${controllerpath}/jobs"
-
-            if [ ! -d "${workpath}" ]
-            then
-                do_not_dir ${workpath}
-            fi
-
-            if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-            then
-                do_reset ${modpath} ${controllerpath} ${workpath}
-            fi
-        done
-    else
-        controllername=${controllernames[${i}]}
-        controllerpath="${modpath}/${controllername}"
-
-        if [ ! -d "${controllerpath}" ]
-        then
-            do_not_dir ${controllerpath}
-        fi
-
-        workpath="${controllerpath}/jobs"
-
-        if [ ! -d "${workpath}" ]
-        then
-            do_not_dir ${workpath}
-        fi
-
-        if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-        then
-            do_reset ${modpath} ${controllerpath} ${workpath}
-        fi
-    fi
-done
\ No newline at end of file
+modpath=$1
+controllerpath=$2
+workpath=$3
+
+modname=$(echo ${modpath} | rev | cut -d'/' -f1 | rev)
+controllername=$(echo ${controllerpath} | rev | cut -d'/' -f1 | rev)
+
+#workpath="${controllerpath}/jobs"
+#mongolinkpath="${SLURMONGO_ROOT}/packages/python/mongolink"
+#toolspath="${SLURMONGO_ROOT}/scripts/tools"
+
+rm ${controllerpath}/*.out ${controllerpath}/*.err tmp* 2>/dev/null
+rm -r ${workpath}/* 2>/dev/null
+rm ${controllerpath}/querystate* 2>/dev/null
+rm ${controllerpath}/reloadstate* 2>/dev/null
+
+echo "JobStep,ExitCode,Resubmit?" > ${controllerpath}/skipped 2>/dev/null
+echo -e "BatchCounter,StepCounter\n1,1" > ${controllerpath}/batchcounter 2>/dev/null
+echo "Pending" > ${controllerpath}/status 2>/dev/null
+
+echo "Finished resetting ${modname}_${controllername}."
+
+#currdir=$(pwd)
+#cd ${mongolinkpath}
+#python setup.py install --user --record filespy.txt
+#sage --python setup.py install --user --record filessage.txt
+#cd ${currdir}
+
+#mongouri=$(cat ${controllerpath}/controller*.job | grep "mongouri=" | cut -d'=' -f2 | sed 's/"//g')
+#basecollection=$(cat ${controllerpath}/controller*.job | grep "basecollection=" | cut -d'=' -f2 | sed 's/"//g')
+#modname=$(cat ${controllerpath}/controller*.job | grep "modname=" | cut -d'=' -f2 | sed 's/"//g')
+#markdone=$(cat ${controllerpath}/controller*.job | grep "markdone=" | cut -d'=' -f2 | sed 's/"//g')
+#h11=$(cat ${controllerpath}/controller*.job | grep "h11=" | cut -d'=' -f2 | sed 's/"//g')
+#python ${toolspath}/unmark.py "${basecollection}" "${modname}" "${markdone}" "{\"H11\":${h11}}"
\ No newline at end of file
diff --git a/bin/submit b/bin/submit
index 61c8417..e1c95c6 100755
--- a/bin/submit
+++ b/bin/submit
@@ -1,156 +1,13 @@
 #!/bin/bash
 
-do_submit () {
-    modpath=$1
-    controllerpath=$2
-    workpath=$3
+modpath=$1
+controllerpath=$2
+workpath=$3
 
-    jobs=$(find ${controllerpath} -path '*/.*' -prune -o -type f -name "*.job" -print 2>/dev/null)
-    for job in ${jobs}
-    do
-        jobname=$(echo ${job} | rev | cut -d'/' -f1 | rev)
-        jobmsg=$(sbatch ${job} 2>&1)
-        echo "${jobmsg} as ${jobname}."
-    done
-}
-
-do_not_dir() {
-    path=$1
-}
-
-modules_dir=$1
-shift
-
-startpath=$(cd ${modules_dir} && pwd -P)
-
-branches=$(find ${startpath} -path '*/.*' -prune -o -type d -exec bash -c 'echo "$(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g" | grep -o / | wc -l) $(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g")"' \; | grep -v '/jobs/' | sort -u | tr '\n' ',')
-maxdepth=$(echo ${branches} | tr ',' '\n' | cut -d' ' -f1 | sort -u | tail -n1)
-leaves=$(echo ${branches} | tr ',' '\n' | grep "^${maxdepth} " | cut -d' ' -f2 | sed 's/\/jobs$//g' | tr '\n' ',')
-moddirpath=$(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1,2 --complement | rev | head -n1)
-modnames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f2 | rev))
-controllernames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1 | rev))
-
-case $# in
-    0)
-        ;;
-    1)
-        if [[ "${moddirpath}" == "${startpath}" ]]
-        then
-            modnames=($1)
-            controllernames=()
-        else
-            stopflag=false
-            for i in ${!controllernames[@]}
-            do
-                if [[ "${controllernames[${i}]}" == "$1" ]]
-                then
-                    modnames=(${modnames[${i}]})
-                    controllernames=(${controllernames[i]})
-                    stopflag=true
-                    break
-                fi
-            done
-            if ! ${stopflag}
-            then
-                for modname in ${modnames[@]}
-                do
-                    if [[ "${modname}" == "$1" ]]
-                    then
-                        modnames=(${modname})
-                        controllernames=()
-                        stopflag=true
-                        break
-                    fi
-                done
-            fi
-            if ! ${stopflag}
-            then
-                for modname in $(find ${moddirpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev)
-                do
-                    if [[ "${modname}" == "$1" ]]
-                    then
-                        modnames=(${modname})
-                        controllernames=()
-                        stopflag=true
-                        break
-                    fi
-                done
-            fi
-            if ! ${stopflag}
-            then
-                for i in ${!modnames[@]}
-                do
-                    modnames=(${modnames[${i}]})
-                    controllernames=($1)
-                done
-            fi
-        fi
-        ;;
-    2)
-        modnames=($1)
-        controllernames=($2)
-        ;;
-    *)
-        echo "Error: You must provide no more than 2 arguments. $# provided."
-        exit
-        ;;
-esac
-
-for i in ${!modnames[@]}
+jobs=$(find ${controllerpath} -type d -path '*/.*' -prune -o -type f -name "*.job" -print 2>/dev/null)
+for job in ${jobs}
 do
-    modname=${modnames[${i}]}
-
-    modpath="${moddirpath}/${modname}"
-
-    if [ ! -d "${modpath}" ]
-    then
-        do_not_dir ${modpath}
-    fi
-
-    if [[ "${controllernames[${i}]}" == "" ]]
-    then
-        controllernames=($(find ${modpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev))
-
-        for controllername in ${controllernames[@]}
-        do
-            controllerpath="${modpath}/${controllername}"
-
-            if [ ! -d "${controllerpath}" ]
-            then
-                do_not_dir ${controllerpath}
-            fi
-
-            workpath="${controllerpath}/jobs"
-
-            if [ ! -d "${workpath}" ]
-            then
-                do_not_dir ${workpath}
-            fi
-
-            if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-            then
-                do_submit ${modpath} ${controllerpath} ${workpath}
-            fi
-        done
-    else
-        controllername=${controllernames[${i}]}
-        controllerpath="${modpath}/${controllername}"
-
-        if [ ! -d "${controllerpath}" ]
-        then
-            do_not_dir ${controllerpath}
-        fi
-
-        workpath="${controllerpath}/jobs"
-
-        if [ ! -d "${workpath}" ]
-        then
-            do_not_dir ${workpath}
-        fi
-
-        if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-        then
-            do_submit ${modpath} ${controllerpath} ${workpath}
-        fi
-    fi
+    jobname=$(echo ${job} | rev | cut -d'/' -f1 | rev)
+    jobmsg=$(sbatch ${job} 2>&1)
+    echo "${jobmsg} as ${jobname}."
 done
\ No newline at end of file
diff --git a/bin/template b/bin/template
index 664f041..1408ab2 100755
--- a/bin/template
+++ b/bin/template
@@ -1,175 +1,30 @@
 #!/bin/bash
 
-do_template () {
-    modpath=$1
-    controllerpath=$2
-    workpath=$3
+modpath=$1
+controllerpath=$2
+workpath=$3
 
-    modname=$(echo ${modpath} | rev | cut -d'/' -f1 | rev)
-    controllername=$(echo ${controllerpath} | rev | cut -d'/' -f1 | rev)
+modname=$(echo ${modpath} | rev | cut -d'/' -f1 | rev)
+controllername=$(echo ${controllerpath} | rev | cut -d'/' -f1 | rev)
 
-    modulespath="${CRUNCH_ROOT}/modules/modules"
+modulespath="${CRUNCH_ROOT}/modules/modules"
 
-    #cp "${binpath}/reset.bash" "${controllerpath}/"
-    cp -T "${modulespath}/${modname}/${modname}_slurm.job" "${controllerpath}/crunch_${modname}_${controllername}_controller.job"
-    echo "JobStep,ExitCode,Resubmit?" > ${controllerpath}/skipped 2>/dev/null
-    echo -e "BatchCounter,StepCounter\n1,1" > ${controllerpath}/batchcounter 2>/dev/null
-    echo "Pending" > ${controllerpath}/status 2>/dev/null
+#cp "${binpath}/reset.bash" "${controllerpath}/"
+cp -T "${modulespath}/${modname}/${modname}_slurm.job" "${controllerpath}/crunch_${modname}_${controllername}_controller.job"
+echo "JobStep,ExitCode,Resubmit?" > ${controllerpath}/skipped 2>/dev/null
+echo -e "BatchCounter,StepCounter\n1,1" > ${controllerpath}/batchcounter 2>/dev/null
+echo "Pending" > ${controllerpath}/status 2>/dev/null
 
-    modulesworkpath=$(echo ${modpath} | rev | cut -d'/' -f1 --complement | rev)
+modulesworkpath=$(echo ${modpath} | rev | cut -d'/' -f1 --complement | rev)
 
-    files=$(find ${controllerpath} -mindepth 1 -path '*/.*' -prune -o -type f -print 2>/dev/null)
-    for file in ${files}
-    do
-        #perl -i -pe 's|#SBATCH(.*)\${CRUNCH_ROOT}|#SBATCH\1'"${CRUNCH_ROOT}"'|g' ${file}
-        sed -i "s|path\_to\_module|${modulesworkpath}|g" ${file}
-        sed -i "s|template|${controllername}|g" ${file}
-        #newfile=$(echo "${file}" | sed "s/template/${controllername}/g")
-        #mv ${file} ${newfile} 2>/dev/null
-    done
-
-    echo "Finished creating template for ${modname}_${controllername}."
-}
-
-do_not_dir() {
-    path=$1
-
-    mkdir ${path}
-}
-
-modules_dir=$1
-shift
-
-startpath=$(cd ${modules_dir} && pwd -P)
-
-branches=$(find ${startpath} -path '*/.*' -prune -o -type d -exec bash -c 'echo "$(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g" | grep -o / | wc -l) $(echo '{}' 2>/dev/null | sed "s/\/jobs\$//g")"' \; | grep -v '/jobs/' | sort -u | tr '\n' ',')
-maxdepth=$(echo ${branches} | tr ',' '\n' | cut -d' ' -f1 | sort -u | tail -n1)
-leaves=$(echo ${branches} | tr ',' '\n' | grep "^${maxdepth} " | cut -d' ' -f2 | sed 's/\/jobs$//g' | tr '\n' ',')
-moddirpath=$(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1,2 --complement | rev | head -n1)
-modnames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f2 | rev))
-controllernames=($(echo ${leaves} | tr ',' '\n' | rev | cut -d'/' -f1 | rev))
-
-case $# in
-    0)
-        ;;
-    1)
-        if [[ "${moddirpath}" == "${startpath}" ]]
-        then
-            modnames=($1)
-            controllernames=()
-        else
-            stopflag=false
-            for i in ${!controllernames[@]}
-            do
-                if [[ "${controllernames[${i}]}" == "$1" ]]
-                then
-                    modnames=(${modnames[${i}]})
-                    controllernames=(${controllernames[i]})
-                    stopflag=true
-                    break
-                fi
-            done
-            if ! ${stopflag}
-            then
-                for modname in ${modnames[@]}
-                do
-                    if [[ "${modname}" == "$1" ]]
-                    then
-                        modnames=(${modname})
-                        controllernames=()
-                        stopflag=true
-                        break
-                    fi
-                done
-            fi
-            if ! ${stopflag}
-            then
-                for modname in $(find ${moddirpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev)
-                do
-                    if [[ "${modname}" == "$1" ]]
-                    then
-                        modnames=(${modname})
-                        controllernames=()
-                        stopflag=true
-                        break
-                    fi
-                done
-            fi
-            if ! ${stopflag}
-            then
-                for i in ${!modnames[@]}
-                do
-                    modnames=(${modnames[${i}]})
-                    controllernames=($1)
-                done
-            fi
-        fi
-        ;;
-    2)
-        modnames=($1)
-        controllernames=($2)
-        ;;
-    *)
-        echo "Error: You must provide no more than 2 arguments. $# provided."
-        exit
-        ;;
-esac
-
-for i in ${!modnames[@]}
+files=$(find ${controllerpath} -mindepth 1 -type d -path '*/.*' -prune -o -type f -print 2>/dev/null)
+for file in ${files}
 do
-    modname=${modnames[${i}]}
-
-    modpath="${moddirpath}/${modname}"
-
-    if [ ! -d "${modpath}" ]
-    then
-        do_not_dir ${modpath}
-    fi
-
-    if [[ "${controllernames[${i}]}" == "" ]]
-    then
-        controllernames=($(find ${modpath} -mindepth 1 -maxdepth 1 -path '*/.*' -prune -o -type d -print 2>/dev/null | rev | cut -d'/' -f1 | rev))
-
-        for controllername in ${controllernames[@]}
-        do
-            controllerpath="${modpath}/${controllername}"
-
-            if [ ! -d "${controllerpath}" ]
-            then
-                do_not_dir ${controllerpath}
-            fi
-
-            workpath="${controllerpath}/jobs"
-
-            if [ ! -d "${workpath}" ]
-            then
-                do_not_dir ${workpath}
-            fi
-
-            if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-            then
-                do_template ${modpath} ${controllerpath} ${workpath}
-            fi
-        done
-    else
-        controllername=${controllernames[${i}]}
-        controllerpath="${modpath}/${controllername}"
-
-        if [ ! -d "${controllerpath}" ]
-        then
-            do_not_dir ${controllerpath}
-        fi
-
-        workpath="${controllerpath}/jobs"
-
-        if [ ! -d "${workpath}" ]
-        then
-            do_not_dir ${workpath}
-        fi
-
-        if [ -d "${modpath}" ] && [ -d "${controllerpath}" ] && [ -d "${workpath}" ]
-        then
-            do_template ${modpath} ${controllerpath} ${workpath}
-        fi
-    fi
-done
\ No newline at end of file
+    #perl -i -pe 's|#SBATCH(.*)\${CRUNCH_ROOT}|#SBATCH\1'"${CRUNCH_ROOT}"'|g' ${file}
+    sed -i "s|path\_to\_module|${modulesworkpath}|g" ${file}
+    sed -i "s|template|${controllername}|g" ${file}
+    #newfile=$(echo "${file}" | sed "s/template/${controllername}/g")
+    #mv ${file} ${newfile} 2>/dev/null
+done
+
+echo "Finished creating template for ${modname}_${controllername}."
\ No newline at end of file
diff --git a/bin/test b/bin/test
new file mode 100755
index 0000000..7aa914d
--- /dev/null
+++ b/bin/test
@@ -0,0 +1,10 @@
+#!/bin/bash
+
+modpath=$1
+controllerpath=$2
+workpath=$3
+
+echo ""
+echo ${modpath}
+echo ${controllerpath}
+echo ${workpath}
\ No newline at end of file
diff --git a/bin/tools/jobgraph.py b/bin/tools/jobgraph.py
index 4987516..315be7b 100644
--- a/bin/tools/jobgraph.py
+++ b/bin/tools/jobgraph.py
@@ -1,6 +1,6 @@
 #!/shared/apps/python/Python-2.7.5/INSTALL/bin/python
 
-import sys,re,mongolink,matplotlib;
+import sys,re,mongojoin,matplotlib;
 import networkx as nx;
 matplotlib.use('Agg');
 import matplotlib.pyplot as plt;
@@ -48,13 +48,13 @@ try:
         expandedlast=list(reversed([leavespair[1][:i+1] for i in range(len(leavespair[1])-1)]));
         expandedleaves+=[expandedfirst+leavesbatch+expandedlast];
 
-    orderedexpandedleaves=mongolink.deldup([y for x in expandedleaves for y in x]);
+    orderedexpandedleaves=mongojoin.deldup([y for x in expandedleaves for y in x]);
     expandedleavesindexes=[[orderedexpandedleaves.index(y) for y in x] for x in expandedleaves];
     expandedleavessplitindexes=[];
     for i in range(len(expandedleaves)):
         expandedleavessplitindexbatch=[z+max([0]+[y for x in expandedleavessplitindexes for y in x if len(x)>0])-expandedleavesindexes[i][0]+1 for z in expandedleavesindexes[i]];
         expandedleavessplitindexes+=[expandedleavessplitindexbatch];
-    expandedleavesitems=mongolink.deldup([(expandedleavessplitindexes[i][j],expandedleaves[i][j]) for i in range(len(expandedleaves)) for j in range(len(expandedleaves[i]))]);
+    expandedleavesitems=mongojoin.deldup([(expandedleavessplitindexes[i][j],expandedleaves[i][j]) for i in range(len(expandedleaves)) for j in range(len(expandedleaves[i]))]);
 
     expandedleavespathrules=[(x[i],x[i+1]) for x in expandedleavessplitindexes for i in range(len(x)-1)];
     indexestolabels=[(x[0],x[1][-1]) for x in expandedleavesitems];
diff --git a/bin/wrapper.py b/bin/wrapper.py
index a131721..aa63919 100644
--- a/bin/wrapper.py
+++ b/bin/wrapper.py
@@ -8,7 +8,7 @@
 #    the Free Software Foundation, either version 3 of the License, or
 #    (at your option) any later version.
 #
-#    This program is distributed in the hope that it will be useful,
+#    This program is distributed in the hope that it will be useful, 
 #    but WITHOUT ANY WARRANTY; without even the implied warranty of
 #    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 #    GNU General Public License for more details.
@@ -16,16 +16,17 @@
 #    You should have received a copy of the GNU General Public License
 #    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 
-import sys,os,glob,json,yaml,re,tempfile;#,linecache,traceback,fcntl
-from time import time,sleep;
-from random import randint;
-from subprocess import Popen,PIPE;
-from threading  import Thread;
-from argparse import ArgumentParser,REMAINDER;
+import sys, os, glob, json, yaml, re, tempfile, datetime#, linecache, traceback, fcntl
+from pprint import pprint
+from time import time, sleep
+from random import randint
+from subprocess import Popen, PIPE
+from threading  import Thread
+from argparse import ArgumentParser, REMAINDER
 try:
-    from Queue import Queue,Empty;
+    from Queue import Queue, Empty
 except ImportError:
-    from queue import Queue,Empty;  # python 3.x
+    from queue import Queue, Empty  # python 3.x
 
 class AsynchronousThreadStreamReader(Thread):
     '''Class to implement asynchronously read output of
@@ -56,7 +57,7 @@ class AsynchronousThreadStreamReaderWriter(Thread):
     '''
     def __init__(self, in_stream, out_stream, in_iter_arg, in_iter_file, in_queue, temp_queue, out_queue, delimiter = '', cleanup = None, time_limit = None, start_time = None):
         assert hasattr(in_iter_arg, '__iter__')
-        assert isinstance(in_iter_file, file) or in_iter_file==None
+        assert isinstance(in_iter_file, file) or in_iter_file == None
         assert isinstance(in_queue, Queue)
         assert isinstance(out_queue, Queue)
         assert callable(in_stream.write)
@@ -88,13 +89,13 @@ class AsynchronousThreadStreamReaderWriter(Thread):
                 in_line = self._inqueue.get()
                 if in_line == "":
                     self._instream.close()
-                    #print("finally!");
-                    #sys.stdout.flush();
+                    #print("finally!")
+                    #sys.stdout.flush()
                 else:
                     self._tempqueue.put(in_line)
-                    self._instream.write(in_line+self._delimiter)
-                    #print("a: "+in_line+self._delimiter);
-                    #sys.stdout.flush();
+                    self._instream.write(in_line + self._delimiter)
+                    #print("a: " + in_line + self._delimiter)
+                    #sys.stdout.flush()
                     self._instream.flush()
             else:
                 in_line = self._initerfile.readline().rstrip("\n")
@@ -106,22 +107,22 @@ class AsynchronousThreadStreamReaderWriter(Thread):
                     in_line = self._inqueue.get()
                     if in_line == "":
                         self._instream.close()
-                        #print("finally!");
-                        #sys.stdout.flush();
+                        #print("finally!")
+                        #sys.stdout.flush()
                     else:
                         self._tempqueue.put(in_line)
-                        self._instream.write(in_line+self._delimiter)
-                        #print("a: "+in_line+self._delimiter);
-                        #sys.stdout.flush();
+                        self._instream.write(in_line + self._delimiter)
+                        #print("a: " + in_line + self._delimiter)
+                        #sys.stdout.flush()
                         self._instream.flush()
                 else:
                     self._tempqueue.put(in_line)
-                    self._instream.write(in_line+self._delimiter)
-                    #print("a: "+in_line+self._delimiter);
-                    #sys.stdout.flush();
+                    self._instream.write(in_line + self._delimiter)
+                    #print("a: " + in_line + self._delimiter)
+                    #sys.stdout.flush()
                     self._instream.flush()
-                    if (self._cleanup != None and self._counter >= self._cleanup) or (self._timelimit != None and self._starttime != None and time()-self._starttime >= self._timelimit):
-                        with tempfile.NamedTemporaryFile(dir="/".join(self._initerfile.name.split("/")[:-1]), delete=False) as tempstream:
+                    if (self._cleanup != None and self._counter >= self._cleanup) or (self._timelimit != None and self._starttime != None and time() - self._starttime >= self._timelimit):
+                        with tempfile.NamedTemporaryFile(dir = "/".join(self._initerfile.name.split("/")[:-1]), delete = False) as tempstream:
                             in_line = self._initerfile.readline()
                             while in_line != "":
                                 tempstream.write(in_line)
@@ -134,18 +135,18 @@ class AsynchronousThreadStreamReaderWriter(Thread):
                         self._counter = 0
         else:
             self._tempqueue.put(in_line)
-            self._instream.write(in_line+self._delimiter)
-            #print("a: "+in_line+self._delimiter);
-            #sys.stdout.flush();
+            self._instream.write(in_line + self._delimiter)
+            #print("a: " + in_line + self._delimiter)
+            #sys.stdout.flush()
             self._instream.flush()
             #self._instream.close()
-            #print(in_line);
-            #sys.stdout.flush();
+            #print(in_line)
+            #sys.stdout.flush()
         for out_line in iter(self._outstream.readline, ''):
             out_line = out_line.rstrip("\n")
             #if out_line == "\n".decode('string_escape'):
-            #print(out_line);
-            #sys.stdout.flush();
+            #print(out_line)
+            #sys.stdout.flush()
             if out_line == "@":
                 try:
                     in_line = self._initerarg.next()
@@ -155,13 +156,13 @@ class AsynchronousThreadStreamReaderWriter(Thread):
                         in_line = self._inqueue.get()
                         if in_line == "":
                             self._instream.close()
-                            #print("finally!");
-                            #sys.stdout.flush();
+                            #print("finally!")
+                            #sys.stdout.flush()
                         else:
                             self._tempqueue.put(in_line)
-                            self._instream.write(in_line+self._delimiter)
-                            #print("a: "+in_line+self._delimiter);
-                            #sys.stdout.flush();
+                            self._instream.write(in_line + self._delimiter)
+                            #print("a: " + in_line + self._delimiter)
+                            #sys.stdout.flush()
                             self._instream.flush()
                     else:
                         in_line = self._initerfile.readline().rstrip("\n")
@@ -173,22 +174,22 @@ class AsynchronousThreadStreamReaderWriter(Thread):
                             in_line = self._inqueue.get()
                             if in_line == "":
                                 self._instream.close()
-                                #print("finally!");
-                                #sys.stdout.flush();
+                                #print("finally!")
+                                #sys.stdout.flush()
                             else:
                                 self._tempqueue.put(in_line)
-                                self._instream.write(in_line+self._delimiter)
-                                #print("a: "+in_line+self._delimiter);
-                                #sys.stdout.flush();
+                                self._instream.write(in_line + self._delimiter)
+                                #print("a: " + in_line + self._delimiter)
+                                #sys.stdout.flush()
                                 self._instream.flush()
                         else:
                             self._tempqueue.put(in_line)
-                            self._instream.write(in_line+self._delimiter)
-                            #print("a: "+in_line+self._delimiter);
-                            #sys.stdout.flush();
+                            self._instream.write(in_line + self._delimiter)
+                            #print("a: " + in_line + self._delimiter)
+                            #sys.stdout.flush()
                             self._instream.flush()
-                            if (self._cleanup != None and self._counter >= self._cleanup) or (self._timelimit != None and self._starttime != None and time()-self._starttime >= self._timelimit):
-                                with tempfile.NamedTemporaryFile(dir="/".join(self._initerfile.name.split("/")[:-1]), delete=False) as tempstream:
+                            if (self._cleanup != None and self._counter >= self._cleanup) or (self._timelimit != None and self._starttime != None and time() - self._starttime >= self._timelimit):
+                                with tempfile.NamedTemporaryFile(dir = "/".join(self._initerfile.name.split("/")[:-1]), delete = False) as tempstream:
                                     in_line = self._initerfile.readline()
                                     while in_line != "":
                                         tempstream.write(in_line)
@@ -203,9 +204,9 @@ class AsynchronousThreadStreamReaderWriter(Thread):
                         self._counter += 1
                 else:
                     self._tempqueue.put(in_line)
-                    self._instream.write(in_line+self._delimiter)
-                    #print("a: "+in_line+self._delimiter);
-                    #sys.stdout.flush();
+                    self._instream.write(in_line + self._delimiter)
+                    #print("a: " + in_line + self._delimiter)
+                    #sys.stdout.flush()
                     self._instream.flush()
             self._outqueue.put(out_line.rstrip("\n"))
 
@@ -224,48 +225,48 @@ class AsynchronousThreadStatsReader(Thread):
     def __init__(self, pid, stats, stats_delay = 0):
         Thread.__init__(self)
         self._pid = str(pid)
-        self._smaps = open("/proc/"+str(pid)+"/smaps","r")
-        self._stat = open("/proc/"+str(pid)+"/stat","r")
-        self._uptime = open("/proc/uptime","r")
-        self._stats = dict((s,0) for s in stats)
-        self._maxstats = dict((s,0) for s in stats)
-        self._totstats = dict((s,0) for s in stats)
+        self._smaps = open("/proc/" + str(pid) + "/smaps", "r")
+        self._stat = open("/proc/" + str(pid) + "/stat", "r")
+        self._uptime = open("/proc/uptime", "r")
+        self._stats = dict((s, 0) for s in stats)
+        self._maxstats = dict((s, 0) for s in stats)
+        self._totstats = dict((s, 0) for s in stats)
         self._nstats = 0
         self._statsdelay = stats_delay
         self.daemon = True
 
     def is_inprog(self):
         '''Check whether there is no more content to expect.'''
-        return self.is_alive() and os.path.exists("/proc/"+self._pid+"/smaps")
+        return self.is_alive() and os.path.exists("/proc/" + self._pid + "/smaps")
 
     def run(self):
         '''The body of the thread: read lines and put them on the queue.'''
         lower_keys = [k.lower() for k in self._stats.keys()]
-        time_keys = ["elapsedtime","totalcputime","parentcputime","childcputime","parentcpuusage","childcpuusage","totalcpuusage"]
+        time_keys = ["elapsedtime", "totalcputime", "parentcputime", "childcputime", "parentcpuusage", "childcpuusage", "totalcpuusage"]
         if any([k in lower_keys for k in time_keys]):
             hz = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
         while self.is_inprog():
             #for stat_name in self._stats.keys():
             #    self._stats[stat_name] = 0
             if any([k in lower_keys for k in time_keys]):
-                self._stat.seek(0);
+                self._stat.seek(0)
                 stat_line = self._stat.read() if self.is_inprog() else ""
-                #print(stat_line);
-                #sys.stdout.flush();
+                #print(stat_line)
+                #sys.stdout.flush()
                 if stat_line != "":
                     stat_line_split = stat_line.split()
-                    utime, stime, cutime, cstime = [(float(f)/hz) for f in stat_line_split[13:17]]
-                    starttime = float(stat_line_split[21])/hz
-                    parent_cputime = utime+stime
-                    child_cputime = cutime+cstime
-                    total_cputime = parent_cputime+child_cputime
-                    self._uptime.seek(0);
+                    utime, stime, cutime, cstime = [(float(f) / hz) for f in stat_line_split[13:17]]
+                    starttime = float(stat_line_split[21]) / hz
+                    parent_cputime = utime + stime
+                    child_cputime = cutime + cstime
+                    total_cputime = parent_cputime + child_cputime
+                    self._uptime.seek(0)
                     uptime_line = self._uptime.read()
                     uptime = float(uptime_line.split()[0])
                     elapsedtime = uptime-starttime
-                    parent_cpuusage = 100*parent_cputime/elapsedtime if elapsedtime > 0 else 0
-                    child_cpuusage = 100*child_cputime/elapsedtime if elapsedtime > 0 else 0
-                    total_cpuusage = 100*total_cputime/elapsedtime if elapsedtime > 0 else 0
+                    parent_cpuusage = 100 * parent_cputime / elapsedtime if elapsedtime > 0 else 0
+                    child_cpuusage = 100 * child_cputime / elapsedtime if elapsedtime > 0 else 0
+                    total_cpuusage = 100 * total_cputime / elapsedtime if elapsedtime > 0 else 0
                     for k in self._stats.keys():
                         if k.lower() == "elapsedtime":
                             self._stats[k] = elapsedtime
@@ -281,8 +282,8 @@ class AsynchronousThreadStatsReader(Thread):
                             self._stats[k] = child_cpuusage
                         if k.lower() == "totalcpuusage":
                             self._stats[k] = total_cpuusage
-            self._smaps.seek(0);
-            smaps_lines = self._smaps.readlines() if self.is_inprog() and os.path.exists("/proc/"+self._pid) else ""
+            self._smaps.seek(0)
+            smaps_lines = self._smaps.readlines() if self.is_inprog() and os.path.exists("/proc/" + self._pid) else ""
             for smaps_line in smaps_lines:
                 smaps_line_split = smaps_line.split()
                 if len(smaps_line_split) == 3:
@@ -292,15 +293,15 @@ class AsynchronousThreadStatsReader(Thread):
                         stat_size = int(stat_size)
                         if stat_unit.lower() == 'b':
                             multiplier = 1
-                        elif stat_unit.lower() in ['k','kb']:
+                        elif stat_unit.lower() in ['k', 'kb']:
                             multiplier = 1024
-                        elif stat_unit.lower() in ['m','mb']:
-                            multiplier = 1000*1024
-                        elif stat_unit.lower() in ['g','gb']:
-                            multiplier = 1000*1000*1024
+                        elif stat_unit.lower() in ['m', 'mb']:
+                            multiplier = 1000 * 1024
+                        elif stat_unit.lower() in ['g', 'gb']:
+                            multiplier = 1000 * 1000 * 1024
                         else:
-                            raise Exception(stat_name+" in "+self._stream.name+" has unrecognized unit: "+stat_unit)
-                        self._stats[stat_name] += multiplier*stat_size
+                            raise Exception(stat_name + " in " + self._stream.name + " has unrecognized unit: " + stat_unit)
+                        self._stats[stat_name] += multiplier * stat_size
                 #smaps_line = self._smaps.readline() if self.is_alive() else ""
             self._nstats += 1
             for k in self._stats.keys():
@@ -330,803 +331,818 @@ class AsynchronousThreadStatsReader(Thread):
 
     def avg_stat(self, stat_name):
         '''Check whether there is no more content to expect.'''
-        return self._totstats[stat_name]/self._nstats if self._nstats > 0 else 0
+        return self._totstats[stat_name] / self._nstats if self._nstats > 0 else 0
 
     def avg_stats(self):
         '''Check whether there is no more content to expect.'''
-        return dict((k,self._totstats[k]/self._nstats if self._nstats > 0 else 0) for k in self._totstats.keys())
+        return dict((k, self._totstats[k] / self._nstats if self._nstats > 0 else 0) for k in self._totstats.keys())
 
-def timestamp2unit(timestamp,unit="seconds"):
-    if timestamp=="infinite":
-        return timestamp;
+def timestamp2unit(timestamp, unit = "seconds"):
+    if timestamp == "infinite":
+        return timestamp
     else:
-        days=0;
+        days = 0
         if "-" in timestamp:
-            daysstr,timestamp=timestamp.split("-");
-            days=int(daysstr);
-        hours,minutes,seconds=[int(x) for x in timestamp.split(":")];
-        hours+=days*24;
-        minutes+=hours*60;
-        seconds+=minutes*60;
-        if unit=="seconds":
-            return seconds;
-        elif unit=="minutes":
-            return float(seconds)/60.;
-        elif unit=="hours":
-            return float(seconds)/(60.*60.);
-        elif unit=="days":
-            return float(seconds)/(60.*60.*24.);
+            daysstr, timestamp = timestamp.split("-")
+            days = int(daysstr)
+        hours, minutes, seconds = [int(x) for x in timestamp.split(":")]
+        hours += days * 24
+        minutes += hours * 60
+        seconds += minutes * 60
+        if unit == "seconds":
+            return seconds
+        elif unit == "minutes":
+            return float(seconds) / 60.
+        elif unit == "hours":
+            return float(seconds) / (60. * 60.)
+        elif unit == "days":
+            return float(seconds) / (60. * 60. * 24.)
         else:
-            return 0;
-
-parser=ArgumentParser();
-
-parser.add_argument('--mod',dest='modname',action='store',default=None,help='');
-parser.add_argument('--controller',dest='controllername',action='store',default=None,help='');
-parser.add_argument('--stepid',dest='stepid',action='store',default="1",help='');
-
-parser.add_argument('--nbatch','-n',dest='nbatch',action='store',default=1,help='');
-parser.add_argument('--nworkers','-N',dest='nworkers',action='store',default=1,help='');
-parser.add_argument('--random-nbatch',dest='random_nbatch',action='store_true',default=False,help='');
-
-parser.add_argument('--dbtype',dest='dbtype',action='store',default=None,help='');
-parser.add_argument('--dbhost',dest='dbhost',action='store',default=None,help='');
-parser.add_argument('--dbport',dest='dbport',action='store',default=None,help='');
-parser.add_argument('--dbusername',dest='dbusername',action='store',default=None,help='');
-parser.add_argument('--dbpassword',dest='dbpassword',action='store',default=None,help='');
-parser.add_argument('--dbname',dest='dbname',action='store',default=None,help='');
-
-parser.add_argument('--logging',dest='logging',action='store_true',default=False,help='');
-parser.add_argument('--temp-local','-t',dest='templocal',action='store_true',default=False,help='');
-parser.add_argument('--write-local','-w',dest='writelocal',action='store_true',default=False,help='');
-parser.add_argument('--write-db','-W',dest='writedb',action='store_true',default=False,help='');
-parser.add_argument('--stats-local','-s',dest='statslocal',action='store_true',default=False,help='');
-parser.add_argument('--stats-db','-S',dest='statsdb',action='store_true',default=False,help='');
-parser.add_argument('--basecoll',dest='basecoll',action='store',default=None,help='');
-parser.add_argument('--dbindexes',dest='dbindexes',nargs='+',default=None,help='');
-parser.add_argument('--markdone',dest='markdone',action='store',default="MARK",help='');
-
-parser.add_argument('--delay',dest='delay',action='store',default=0,help='');
-parser.add_argument('--stats',dest='stats_list',nargs='+',action='store',default=[],help='');
-parser.add_argument('--stats-delay',dest='stats_delay',action='store',default=0,help='');
-parser.add_argument('--delimiter','-d',dest='delimiter',action='store',default='\n',help='');
-parser.add_argument('--input','-i',dest='input_list',nargs='+',action='store',default=[],help='');
-parser.add_argument('--file','-f',dest='input_file',action='store',default=None,help='');
-parser.add_argument('--cleanup-after',dest='cleanup',action='store',default=None,help='');
-parser.add_argument('--interactive',dest='interactive',action='store_true',default=False,help='');
-parser.add_argument('--time-limit',dest='time_limit',action='store',default=None,help='');
-parser.add_argument('--ignored-strings',dest='ignoredstrings',nargs='+',default=[],help='');
-parser.add_argument('--script-language',dest='scriptlanguage',action='store',default=None,help='');
-parser.add_argument('--script','-c',dest='scriptcommand',nargs='+',required=True,help='');
-parser.add_argument('--args','-a',dest='scriptargs',nargs=REMAINDER,default=[],help='');
-
-kwargs=vars(parser.parse_known_args()[0]);
-
-#print(kwargs['input_list']);
-#sys.stdout.flush();
-
-if kwargs['time_limit']==None:
-    start_time=None;
+            return 0
+
+parser = ArgumentParser()
+
+parser.add_argument('--mod', dest = 'modname', action = 'store', default = None, help = '')
+parser.add_argument('--controller', dest = 'controllername', action = 'store', default = None, help = '')
+parser.add_argument('--stepid', dest = 'stepid', action = 'store', default = "1", help = '')
+
+parser.add_argument('--nbatch', '-n', dest = 'nbatch', action = 'store', default = 1, help = '')
+parser.add_argument('--nworkers', '-N', dest = 'nworkers', action = 'store', default = 1, help = '')
+parser.add_argument('--random-nbatch', dest = 'random_nbatch', action = 'store_true', default = False, help = '')
+
+parser.add_argument('--dbtype', dest = 'dbtype', action = 'store', default = None, help = '')
+parser.add_argument('--dbhost', dest = 'dbhost', action = 'store', default = None, help = '')
+parser.add_argument('--dbport', dest = 'dbport', action = 'store', default = None, help = '')
+parser.add_argument('--dbusername', dest = 'dbusername', action = 'store', default = None, help = '')
+parser.add_argument('--dbpassword', dest = 'dbpassword', action = 'store', default = None, help = '')
+parser.add_argument('--dbname', dest = 'dbname', action = 'store', default = None, help = '')
+
+parser.add_argument('--logging', dest = 'logging', action = 'store_true', default = False, help = '')
+parser.add_argument('--temp-local', '-t', dest = 'templocal', action = 'store_true', default = False, help = '')
+parser.add_argument('--write-local', '-w', dest = 'writelocal', action = 'store_true', default = False, help = '')
+parser.add_argument('--write-db', '-W', dest = 'writedb', action = 'store_true', default = False, help = '')
+parser.add_argument('--stats-local', '-s', dest = 'statslocal', action = 'store_true', default = False, help = '')
+parser.add_argument('--stats-db', '-S', dest = 'statsdb', action = 'store_true', default = False, help = '')
+parser.add_argument('--basecoll', dest = 'basecoll', action = 'store', default = None, help = '')
+parser.add_argument('--dbindexes', dest = 'dbindexes', nargs = '+', default = None, help = '')
+parser.add_argument('--markdone', dest = 'markdone', action = 'store', default = "MARK", help = '')
+
+parser.add_argument('--delay', dest = 'delay', action = 'store', default = 0, help = '')
+parser.add_argument('--stats', dest = 'stats_list', nargs = '+', action = 'store', default = [], help = '')
+parser.add_argument('--stats-delay', dest = 'stats_delay', action = 'store', default = 0, help = '')
+parser.add_argument('--delimiter', '-d', dest = 'delimiter', action = 'store', default = '\n', help = '')
+parser.add_argument('--input', '-i', dest = 'input_list', nargs = '+', action = 'store', default = [], help = '')
+parser.add_argument('--file', '-f', dest = 'input_file', action = 'store', default = None, help = '')
+parser.add_argument('--cleanup-after', dest = 'cleanup', action = 'store', default = None, help = '')
+parser.add_argument('--interactive', dest = 'interactive', action = 'store_true', default = False, help = '')
+parser.add_argument('--time-limit', dest = 'time_limit', action = 'store', default = None, help = '')
+parser.add_argument('--ignored-strings', dest = 'ignoredstrings', nargs = '+', default = [], help = '')
+parser.add_argument('--script-language', dest = 'scriptlanguage', action = 'store', default = None, help = '')
+parser.add_argument('--script', '-c', dest = 'scriptcommand', nargs = '+', required = True, help = '')
+parser.add_argument('--args', '-a', dest = 'scriptargs', nargs = REMAINDER, default = [], help = '')
+
+kwargs = vars(parser.parse_known_args()[0])
+
+#print(kwargs['input_list'])
+#sys.stdout.flush()
+
+if kwargs['time_limit'] == None:
+    start_time = None
 else:
-    start_time=time();
-    kwargs['time_limit']=timestamp2unit(kwargs['time_limit']);
-
-kwargs['delay']=float(kwargs['delay']);
-kwargs['stats_delay']=float(kwargs['stats_delay']);
-kwargs['nbatch']=int(kwargs['nbatch']);
-kwargs['nworkers']=int(kwargs['nworkers']);
-#kwargs['delimiter']=kwargs['delimiter']#.decode("string_escape");
-if kwargs['cleanup']=="":
-    kwargs['cleanup']=None;
+    start_time = time()
+    kwargs['time_limit'] = timestamp2unit(kwargs['time_limit'])
+
+kwargs['delay'] = float(kwargs['delay'])
+kwargs['stats_delay'] = float(kwargs['stats_delay'])
+kwargs['nbatch'] = int(kwargs['nbatch'])
+kwargs['nworkers'] = int(kwargs['nworkers'])
+#kwargs['delimiter'] = kwargs['delimiter']#.decode("string_escape")
+if kwargs['cleanup'] == "":
+    kwargs['cleanup'] = None
 else:
-    kwargs['cleanup']=eval(kwargs['cleanup']);
+    kwargs['cleanup'] = eval(kwargs['cleanup'])
 
-if kwargs['modname']==None:
-    modname=kwargs['scriptcommand'][-1].split("/")[-1].split(".")[0];
+if kwargs['modname'] == None:
+    modname = kwargs['scriptcommand'][-1].split("/")[-1].split(".")[0]
 else:
-    modname=kwargs['modname'];
-
-script=" ".join(kwargs['scriptcommand']+kwargs['scriptargs']);
-
-ignoredstrings=kwargs['ignoredstrings'];
-
-if kwargs['controllername']==None:
-    rootpath=os.getcwd();
-    workpath=rootpath;
-    dbtype=kwargs['dbtype'];
-    dbhost=kwargs['dbhost'];
-    dbport=kwargs['dbport'];
-    dbusername=kwargs['dbusername'];
-    dbpassword=kwargs['dbpassword'];
-    dbname=kwargs['dbname'];
-    basecoll=kwargs['basecoll'];
+    modname = kwargs['modname']
+
+script = " ".join(kwargs['scriptcommand'] + kwargs['scriptargs'])
+
+ignoredstrings = kwargs['ignoredstrings']
+
+if kwargs['controllername'] == None:
+    rootpath = os.getcwd()
+    workpath = rootpath
+    dbtype = kwargs['dbtype']
+    dbhost = kwargs['dbhost']
+    dbport = kwargs['dbport']
+    dbusername = kwargs['dbusername']
+    dbpassword = kwargs['dbpassword']
+    dbname = kwargs['dbname']
+    basecoll = kwargs['basecoll']
 else:
-    rootpath=os.environ['CRUNCH_ROOT'];
-    #softwarefile=rootpath+"/state/software";
-    #with open(softwarefile,"r") as softwarestream:
-    #    softwarestream.readline();
+    rootpath = os.environ['CRUNCH_ROOT']
+    #softwarefile = rootpath + "/state/software"
+    #with open(softwarefile, "r") as softwarestream:
+    #    softwarestream.readline()
     #    for line in softwarestream:
-    #        ext=line.split(',')[2];
-    #        if ext+" " in kwargs['scriptcommand']:
-    #            break;
-    #modulesfile=rootpath+"/state/modules";
-    #with open(modulesfile,"r") as modulesstream:
-    #    modulesstream.readline();
+    #        ext = line.split(',')[2]
+    #        if ext + " " in kwargs['scriptcommand']:
+    #            break
+    #modulesfile = rootpath + "/state/modules"
+    #with open(modulesfile, "r") as modulesstream:
+    #    modulesstream.readline()
     #    for line in modulesstream:
-    #        modname=line.rstrip("\n");
-    #        if " "+modname+ext+" " in kwargs['scriptcommand'] or "/"+modname+ext+" " in kwargs['scriptcommand']:
-    #            break;
-    #controllerpath=rootpath+"/modules/"+modname+"/"+kwargs['controllername'];
-    with open(rootpath+"/config.yaml","r") as configstream:
-        configdoc=yaml.load(configstream);
+    #        modname = line.rstrip("\n")
+    #        if " " + modname + ext + " " in kwargs['scriptcommand'] or "/" + modname + ext + " " in kwargs['scriptcommand']:
+    #            break
+    #controllerpath = rootpath + "/modules/" + modname + "/" + kwargs['controllername']
+    with open(rootpath + "/config.yaml", "r") as configstream:
+        configdoc = yaml.load(configstream)
 
-    if (kwargs['scriptlanguage']!=None) and (kwargs['scriptlanguage'] in configdoc["software"].keys()) and ("ignored-strings" in configdoc["software"][kwargs['scriptlanguage']].keys()):
-        ignoredstrings+=configdoc["software"][kwargs['scriptlanguage']]["ignored-strings"];
+    if (kwargs['scriptlanguage'] != None) and (kwargs['scriptlanguage'] in configdoc["software"].keys()) and ("ignored-strings" in configdoc["software"][kwargs['scriptlanguage']].keys()):
+        ignoredstrings += configdoc["software"][kwargs['scriptlanguage']]["ignored-strings"]
 
-    if configdoc["workload-manager"]=="slurm":
-        from crunch_slurm import *;
+    if configdoc["workload-manager"] == "slurm":
+        from crunch_slurm import *
 
-    controllerpath="/".join(get_controllerpath(kwargs['stepid']).split("/")[:-1]);
-    workpath=controllerpath+"/jobs";
+    controllerpath = "/".join(get_controllerpath(kwargs['stepid']).split("/")[:-1])
+    workpath = controllerpath + "/jobs"
     if not os.path.isdir(workpath):
-        os.mkdir(workpath);
-    controllerfile=controllerpath+"/crunch_"+modname+"_"+kwargs['controllername']+"_controller.job";
-    with open(controllerfile,"r") as controllerstream:
+        os.mkdir(workpath)
+    controllerfile = controllerpath + "/crunch_" + modname + "_" + kwargs['controllername'] + "_controller.job"
+    with open(controllerfile, "r") as controllerstream:
         for controllerline in controllerstream:
             if "dbtype=" in controllerline:
-                dbtype=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
-                if dbtype=="":
-                    dbtype=None;
+                dbtype = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
+                if dbtype == "":
+                    dbtype = None
             elif "dbhost=" in controllerline:
-                dbhost=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
-                if dbhost=="":
-                    dbhost=None;
+                dbhost = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
+                if dbhost == "":
+                    dbhost = None
             elif "dbport=" in controllerline:
-                dbport=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
-                if dbport=="":
-                    dbport=None;
+                dbport = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
+                if dbport == "":
+                    dbport = None
             elif "dbusername=" in controllerline:
-                dbusername=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
-                if dbusername=="":
-                    dbusername=None;
+                dbusername = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
+                if dbusername == "":
+                    dbusername = None
             elif "dbpassword=" in controllerline:
-                dbpassword=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
-                if dbpassword=="":
-                    dbpassword=None;
+                dbpassword = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
+                if dbpassword == "":
+                    dbpassword = None
             elif "dbname=" in controllerline:
-                dbname=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
-                if dbname=="":
-                    dbname=None;
+                dbname = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
+                if dbname == "":
+                    dbname = None
             elif "basecollection=" in controllerline:
-                basecoll=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
-                if basecoll=="":
-                    basecoll=None;
+                basecoll = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
+                if basecoll == "":
+                    basecoll = None
 
-if len(kwargs['input_list'])>0:
-    stdin_iter_arg=iter(kwargs['input_list']);
+if len(kwargs['input_list']) > 0:
+    stdin_iter_arg = iter(kwargs['input_list'])
 else:
-    stdin_iter_arg=iter([]);
+    stdin_iter_arg = iter([])
 
-if kwargs['input_file']!=None:
+if kwargs['input_file'] != None:
     if "/" not in kwargs['input_file']:
-        kwargs['input_file']=workpath+"/"+kwargs['input_file'];
-    stdin_iter_file=open(kwargs['input_file'],"r");
+        kwargs['input_file'] = workpath + "/" + kwargs['input_file']
+    stdin_iter_file = open(kwargs['input_file'], "r")
 else:
-    stdin_iter_file=None;
+    stdin_iter_file = None
 
-if kwargs['input_file']!=None:
-    filename=".".join(kwargs['input_file'].split('.')[:-1]);
+if kwargs['input_file'] != None:
+    filename = ".".join(kwargs['input_file'].split('.')[:-1])
 else:
-    filename=workpath+"/"+kwargs['stepid'];
+    filename = workpath + "/" + kwargs['stepid']
 
 if kwargs['logging']:
-    logiolist=[""]
+    logiolist = [""]
 
 if kwargs['writelocal'] or kwargs['statslocal']:
-    outiolist=[""];
-    outiostream=open(filename+".out","w");
+    outiolist = [""]
+    outiostream = open(filename + ".out", "w")
     if kwargs['templocal']:
-        tempiostream=open(filename+".temp","w");
-    if (kwargs['dbindexes']==None) or ((kwargs['controllername']==None) and (basecoll==None)):
-        parser.error("Both --write-local and --stats-local require either the options:\n--controllername --dbindexes,\nor the options: --basecoll --dbindexes")
+        tempiostream = open(filename + ".temp", "w")
+    if (kwargs['dbindexes'] == None) or ((kwargs['controllername'] == None) and (basecoll == None)):
+        parser.error("Both --write-local and --stats-local require either the options:\n--controllername --dbindexes, \nor the options: --basecoll --dbindexes")
 
-if any([kwargs[x] for x in ['writedb','statsdb']]):
-    if (kwargs['dbindexes']==None) or ((kwargs['controllername']==None) and any([x==None for x in [dbtype,dbhost,dbport,dbusername,dbpassword,dbname,basecoll]])):
-        parser.error("Both --writedb and --statsdb require either the options:\n--controllername --dbindexes,\nor the options:\n--dbtype --dbhost --dbport --dbusername --dbpassword --dbname --basecoll --dbindexes");
+if any([kwargs[x] for x in ['writedb', 'statsdb']]):
+    if (kwargs['dbindexes'] == None) or ((kwargs['controllername'] == None) and any([x == None for x in [dbtype, dbhost, dbport, dbusername, dbpassword, dbname, basecoll]])):
+        parser.error("Both --writedb and --statsdb require either the options:\n--controllername --dbindexes, \nor the options:\n--dbtype --dbhost --dbport --dbusername --dbpassword --dbname --basecoll --dbindexes")
     else:
-        if dbtype=="mongodb":
-            from mongolink import get_bsonsize;
-            from pymongo import MongoClient,UpdateOne,WriteConcern;
-            from pymongo.errors import BulkWriteError;
-            if dbusername==None:
-                dbclient=MongoClient("mongodb://"+dbhost+":"+dbport+"/"+dbname);
+        if dbtype == "mongodb":
+            from mongojoin import get_bsonsize
+            from pymongo import MongoClient, UpdateOne, WriteConcern
+            from pymongo.errors import BulkWriteError
+            if dbusername == None:
+                dbclient = MongoClient("mongodb://" + dbhost + ":" + dbport + "/" + dbname)
             else:
-                dbclient=MongoClient("mongodb://"+dbusername+":"+dbpassword+"@"+dbhost+":"+dbport+"/"+dbname+"?authMechanism=SCRAM-SHA-1");
+                dbclient = MongoClient("mongodb://" + dbusername + ":" + dbpassword + "@" + dbhost + ":" + dbport + "/" + dbname + "?authMechanism=SCRAM-SHA-1")
 
-            db=dbclient[dbname];
+            db = dbclient[dbname]
         else:
-            raise Exception("Only \"mongodb\" is currently supported.");
+            raise Exception("Only \"mongodb\" is currently supported.")
 
-process=Popen(script,shell=True,stdin=PIPE,stdout=PIPE,stderr=PIPE,bufsize=1);
+process = Popen(script, shell = True, stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize = 1)
 
-stdin_queue=Queue();
+stdin_queue = Queue()
 if not kwargs['interactive']:
-    stdin_queue.put("");
+    stdin_queue.put("")
 
-temp_queue=Queue();
+temp_queue = Queue()
 
-stdout_queue=Queue();
-stdout_reader=AsynchronousThreadStreamReaderWriter(process.stdin,process.stdout,stdin_iter_arg,stdin_iter_file,stdin_queue,temp_queue,stdout_queue,delimiter=kwargs['delimiter'],cleanup=kwargs['cleanup'],time_limit=kwargs['time_limit'],start_time=start_time);
-stdout_reader.start();
+stdout_queue = Queue()
+stdout_reader = AsynchronousThreadStreamReaderWriter(process.stdin, process.stdout, stdin_iter_arg, stdin_iter_file, stdin_queue, temp_queue, stdout_queue, delimiter = kwargs['delimiter'], cleanup = kwargs['cleanup'], time_limit = kwargs['time_limit'], start_time = start_time)
+stdout_reader.start()
 
-stderr_queue=Queue();
-stderr_reader=AsynchronousThreadStreamReader(process.stderr,stderr_queue);
-stderr_reader.start();
+stderr_queue = Queue()
+stderr_reader = AsynchronousThreadStreamReader(process.stderr, stderr_queue)
+stderr_reader.start()
 
 if kwargs['statslocal'] or kwargs['statsdb']:
-    stats_reader=AsynchronousThreadStatsReader(process.pid,kwargs['stats_list'],stats_delay=kwargs['stats_delay']);
-    stats_reader.start();
-
-bulkcolls={};
-bulkrequestslist=[{}];
-countallbatches=[0];
-#tempiostream=open(workpath+"/"+stepname+".temp","w");
-bsonsize=0;
-countthisbatch=0;
-nbatch=randint(1,kwargs['nbatch']) if kwargs['random_nbatch'] else kwargs['nbatch'];
-while process.poll()==None and stats_reader.is_inprog() and not (stdout_reader.eof() or stderr_reader.eof()):
+    stats_reader = AsynchronousThreadStatsReader(process.pid, kwargs['stats_list'], stats_delay = kwargs['stats_delay'])
+    stats_reader.start()
+
+bulkcolls = {}
+bulkrequestslist = [{}]
+countallbatches = [0]
+#tempiostream = open(workpath + "/" + stepname + ".temp", "w")
+bsonsize = 0
+countthisbatch = 0
+nbatch = randint(1, kwargs['nbatch']) if kwargs['random_nbatch'] else kwargs['nbatch']
+while process.poll() == None and stats_reader.is_inprog() and not (stdout_reader.eof() or stderr_reader.eof()):
     if stdout_reader.waiting():
-        stdin_line=sys.stdin.readline().rstrip("\n");
-        stdin_queue.put(stdin_line);
+        stdin_line = sys.stdin.readline().rstrip("\n")
+        stdin_queue.put(stdin_line)
 
     while not stdout_queue.empty():
-        while (not stdout_queue.empty()) and ((len(bulkrequestslist)<=1 and countthisbatch<nbatch) or (len(glob.glob(workpath+"/*.lock"))>=kwargs['nworkers'])):
-            line=stdout_queue.get().rstrip("\n");
+        while (not stdout_queue.empty()) and ((len(bulkrequestslist) <= 1 and countthisbatch < nbatch) or (len(glob.glob(workpath + "/*.lock")) >= kwargs['nworkers'])):
+            line = stdout_queue.get().rstrip("\n")
             if line not in ignoredstrings:
-                linehead=re.sub("^([-+&@].*?>|None).*",r"\1",line);
-                linemarker=linehead[0];
-                if linemarker=="-":
-                    newcollection,strindexdoc=linehead[1:-1].split(".");
-                    newindexdoc=json.loads(strindexdoc);
-                    linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
-                    doc=json.loads(linedoc);
-                    if dbtype=="mongodb":
-                        bsonsize-=get_bsonsize(doc);
+                linehead = re.sub("^([-+&@].*?>|None).*", r"\1", line)
+                linemarker = linehead[0]
+                if linemarker == "-":
+                    newcollection, strindexdoc = linehead[1:-1].split(".")
+                    newindexdoc = json.loads(strindexdoc)
+                    linedoc = re.sub("^[-+&@].*?>", "", line).rstrip("\n")
+                    doc = json.loads(linedoc)
+                    if dbtype == "mongodb":
+                        bsonsize -= get_bsonsize(doc)
                     if kwargs['templocal']:
-                        tempiostream.write(line+"\n");
-                        tempiostream.flush();
+                        tempiostream.write(line + "\n")
+                        tempiostream.flush()
                     if kwargs['writelocal']:
-                        outiolist[-1]+=line+"\n";
+                        outiolist[-1] += line + "\n"
                     if kwargs['writedb']:
                         if newcollection not in bulkcolls.keys():
-                            #bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
-                            if dbtype=="mongodb":
-                                bulkcolls[newcollection]=db.get_collection(newcollection,write_concern=WriteConcern(fsync=True));
+                            #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
+                            if dbtype == "mongodb":
+                                bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                         if newcollection not in bulkrequestslist[-1].keys():
-                            bulkrequestslist[-1][newcollection]=[];
-                        #bulkdict[newcollection].find(newindexdoc).update({"$unset":doc});
-                        if dbtype=="mongodb":
-                            bulkrequestslist[-1][newcollection]+=[UpdateOne(newindexdoc,{"$unset":doc})];
-                elif linemarker=="+":
-                    #tempiostream.write(linehead[1:-1].split(".")+"\n");
-                    #sys.stdout.flush();
-                    #print(line);
-                    #print(linehead);
-                    #sys.stdout.flush();
-                    newcollection,strindexdoc=linehead[1:-1].split(".");
-                    newindexdoc=json.loads(strindexdoc);
-                    linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
-                    doc=json.loads(linedoc);
-                    if dbtype=="mongodb":
-                        bsonsize+=get_bsonsize(doc);
+                            bulkrequestslist[-1][newcollection] = []
+                        #bulkdict[newcollection].find(newindexdoc).update({"$unset": doc})
+                        if dbtype == "mongodb":
+                            bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$unset": doc})]
+                elif linemarker == " + ":
+                    #tempiostream.write(linehead[1:-1].split(".") + "\n")
+                    #sys.stdout.flush()
+                    #print(line)
+                    #print(linehead)
+                    #sys.stdout.flush()
+                    newcollection, strindexdoc = linehead[1:-1].split(".")
+                    newindexdoc = json.loads(strindexdoc)
+                    linedoc = re.sub("^[-+&@].*?>", "", line).rstrip("\n")
+                    doc = json.loads(linedoc)
+                    if dbtype == "mongodb":
+                        bsonsize += get_bsonsize(doc)
                     if kwargs['templocal']:
-                        tempiostream.write(line+"\n");
-                        tempiostream.flush();
+                        tempiostream.write(line + "\n")
+                        tempiostream.flush()
                     if kwargs['writelocal']:
-                        outiolist[-1]+=line+"\n";
+                        outiolist[-1] += line + "\n"
                     if kwargs['writedb']:
                         if newcollection not in bulkcolls.keys():
-                            #bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
-                            if dbtype=="mongodb":
-                                bulkcolls[newcollection]=db.get_collection(newcollection,write_concern=WriteConcern(fsync=True));
+                            #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
+                            if dbtype == "mongodb":
+                                bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                         if newcollection not in bulkrequestslist[-1].keys():
-                            bulkrequestslist[-1][newcollection]=[];
-                        #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set":doc});
-                        if dbtype=="mongodb":
-                            bulkrequestslist[-1][newcollection]+=[UpdateOne(newindexdoc,{"$set":doc},upsert=True)];
-                elif linemarker=="&":
-                    newcollection,strindexdoc=linehead[1:-1].split(".");
-                    newindexdoc=json.loads(strindexdoc);
-                    linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
-                    doc=json.loads(linedoc);
-                    if dbtype=="mongodb":
-                        bsonsize+=get_bsonsize(doc);
+                            bulkrequestslist[-1][newcollection] = []
+                        #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set": doc})
+                        if dbtype == "mongodb":
+                            bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$set": doc}, upsert = True)]
+                elif linemarker == "&":
+                    newcollection, strindexdoc = linehead[1:-1].split(".")
+                    newindexdoc = json.loads(strindexdoc)
+                    linedoc = re.sub("^[-+&@].*?>", "", line).rstrip("\n")
+                    doc = json.loads(linedoc)
+                    if dbtype == "mongodb":
+                        bsonsize += get_bsonsize(doc)
                     if kwargs['templocal']:
-                        tempiostream.write(line+"\n");
-                        tempiostream.flush();
+                        tempiostream.write(line + "\n")
+                        tempiostream.flush()
                     if kwargs['writelocal']:
-                        outiolist[-1]+=line+"\n";
+                        outiolist[-1] += line + "\n"
                     if kwargs['writedb']:
                         if newcollection not in bulkcolls.keys():
-                            #bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
-                            if dbtype=="mongodb":
-                                bulkcolls[newcollection]=db.get_collection(newcollection,write_concern=WriteConcern(fsync=True));
+                            #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
+                            if dbtype == "mongodb":
+                                bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                         if newcollection not in bulkrequestslist[-1].keys():
-                            bulkrequestslist[-1][newcollection]=[];
-                        #bulkdict[newcollection].find(newindexdoc).upsert().update({"$addToSet":doc});
-                        if dbtype=="mongodb":
-                            bulkrequestslist[-1][newcollection]+=[UpdateOne(newindexdoc,{"$addToSet":doc},upsert=True)];
-                elif linemarker=="@":
+                            bulkrequestslist[-1][newcollection] = []
+                        #bulkdict[newcollection].find(newindexdoc).upsert().update({"$addToSet": doc})
+                        if dbtype == "mongodb":
+                            bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$addToSet": doc}, upsert = True)]
+                elif linemarker == "@":
                     if kwargs['statslocal'] or kwargs['statsdb']:
-                        cputime="%.2f" % stats_reader.stat("TotalCPUTime");
-                        maxrss=stats_reader.max_stat("Rss");
-                        maxvmsize=stats_reader.max_stat("Size");
-                    #    stats=getstats("sstat",["MaxRSS","MaxVMSize"],kwargs['stepid']);
-                    #    if (len(stats)==1) and (stats[0]==""):
-                    #        newtotcputime,maxrss,maxvmsize=[eval(x) for x in getstats("sacct",["CPUTimeRAW","MaxRSS","MaxVMSize"],kwargs['stepid'])];
+                        cputime = "%.2f" % stats_reader.stat("TotalCPUTime")
+                        maxrss = stats_reader.max_stat("Rss")
+                        maxvmsize = stats_reader.max_stat("Size")
+                    #    stats = getstats("sstat", ["MaxRSS", "MaxVMSize"], kwargs['stepid'])
+                    #    if (len(stats) == 1) and (stats[0] == ""):
+                    #        newtotcputime, maxrss, maxvmsize = [eval(x) for x in getstats("sacct", ["CPUTimeRAW", "MaxRSS", "MaxVMSize"], kwargs['stepid'])]
                     #    else:
-                    #        newtotcputime=eval(getstats("sacct",["CPUTimeRAW"],kwargs['stepid'])[0]);
-                    #        maxrss,maxvmsize=stats;
-                    #    cputime=newtotcputime-totcputime;
-                    #    totcputime=newtotcputime;
-                    #newcollection,strindexdoc=linehead[1:].split("<")[0].split(".");
-                    #newindexdoc=json.loads(strindexdoc);
-                    newcollection=basecoll;
-                    doc=json.loads(temp_queue.get());
-                    newindexdoc=dict([(x,doc[x]) for x in kwargs['dbindexes']]);               
+                    #        newtotcputime = eval(getstats("sacct", ["CPUTimeRAW"], kwargs['stepid'])[0])
+                    #        maxrss, maxvmsize = stats
+                    #    cputime = newtotcputime-totcputime
+                    #    totcputime = newtotcputime
+                    #newcollection, strindexdoc = linehead[1:].split("<")[0].split(".")
+                    #newindexdoc = json.loads(strindexdoc)
+                    newcollection = basecoll
+                    doc = json.loads(temp_queue.get())
+                    newindexdoc = dict([(x, doc[x]) for x in kwargs['dbindexes']]);               
                     if kwargs['logging']:
-                        logiolist[-1]+=line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"<OUT\n";
-                        sys.stdout.write(line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"<TEMP\n");
-                        sys.stdout.flush();
+                        logiolist[-1] += line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "<OUT\n"
+                        sys.stdout.write(datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S") + " UTC: " + line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "<TEMP\n")
+                        sys.stdout.flush()
                     if kwargs['templocal']:
-                        tempiostream.write(line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"\n");
-                        tempiostream.write("CPUTime: "+str(cputime)+" seconds\n");
-                        tempiostream.write("MaxRSS: "+str(maxrss)+" bytes\n");
-                        tempiostream.write("MaxVMSize: "+str(maxvmsize)+" bytes\n");
-                        tempiostream.write("BSONSize: "+str(bsonsize)+" bytes\n");
-                        tempiostream.flush();
+                        tempiostream.write(line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "\n")
+                        tempiostream.write("CPUTime: " + str(cputime) + " seconds\n")
+                        tempiostream.write("MaxRSS: " + str(maxrss) + " bytes\n")
+                        tempiostream.write("MaxVMSize: " + str(maxvmsize) + " bytes\n")
+                        tempiostream.write("BSONSize: " + str(bsonsize) + " bytes\n")
+                        tempiostream.flush()
                     if kwargs['writelocal']:
-                        outiolist[-1]+=line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"\n";
+                        outiolist[-1] += line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "\n"
                     if kwargs['statslocal']:
-                        outiolist[-1]+="CPUTime: "+str(cputime)+" seconds\n";
-                        outiolist[-1]+="MaxRSS: "+str(maxrss)+" bytes\n";
-                        outiolist[-1]+="MaxVMSize: "+str(maxvmsize)+" bytes\n";
-                        outiolist[-1]+="BSONSize: "+str(bsonsize)+" bytes\n";
-                    statsmark={};
+                        outiolist[-1] += "CPUTime: " + str(cputime) + " seconds\n"
+                        outiolist[-1] += "MaxRSS: " + str(maxrss) + " bytes\n"
+                        outiolist[-1] += "MaxVMSize: " + str(maxvmsize) + " bytes\n"
+                        outiolist[-1] += "BSONSize: " + str(bsonsize) + " bytes\n"
+                    statsmark = {}
                     if kwargs['statsdb']:
-                        statsmark.update({modname+"STATS":{"CPUTIME":cputime,"MAXRSS":maxrss,"MAXVMSIZE":maxvmsize,"BSONSIZE":bsonsize}});
-                    if kwargs['markdone']!="":
-                        statsmark.update({modname+kwargs['markdone']:True});
-                    if len(statsmark)>0:
+                        statsmark.update({modname + "STATS": {"CPUTIME": cputime, "MAXRSS": maxrss, "MAXVMSIZE": maxvmsize, "BSONSIZE": bsonsize}})
+                    if kwargs['markdone'] != "":
+                        statsmark.update({modname + kwargs['markdone']: True})
+                    if len(statsmark) > 0:
                         if newcollection not in bulkcolls.keys():
-                            #bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
-                            if dbtype=="mongodb":
-                                bulkcolls[newcollection]=db.get_collection(newcollection,write_concern=WriteConcern(fsync=True));
+                            #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
+                            if dbtype == "mongodb":
+                                bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                         if newcollection not in bulkrequestslist[-1].keys():
-                            bulkrequestslist[-1][newcollection]=[];
-                        #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set":statsmark});
-                        if dbtype=="mongodb":
-                            bulkrequestslist[-1][newcollection]+=[UpdateOne(newindexdoc,{"$set":statsmark},upsert=True)];
-                    bsonsize=0;
-                    countthisbatch+=1;
-                    countallbatches[-1]+=1;
-                    if countthisbatch==nbatch:
-                        bulkrequestslist+=[{}];
+                            bulkrequestslist[-1][newcollection] = []
+                        #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set": statsmark})
+                        if dbtype == "mongodb":
+                            bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$set": statsmark}, upsert = True)]
+                    bsonsize = 0
+                    countthisbatch += 1
+                    countallbatches[-1] += 1
+                    if countthisbatch == nbatch:
+                        bulkrequestslist += [{}]
                         if kwargs['logging']:
-                            logiolist+=[""];
+                            logiolist += [""]
                         if kwargs['writelocal'] or kwargs['statslocal']:
-                            outiolist+=[""];
-                        countthisbatch=0;
-                        nbatch=randint(1,kwargs['nbatch']) if kwargs['random_nbatch'] else kwargs['nbatch'];
-                        countallbatches+=[0];
+                            outiolist += [""]
+                        countthisbatch = 0
+                        nbatch = randint(1, kwargs['nbatch']) if kwargs['random_nbatch'] else kwargs['nbatch']
+                        countallbatches += [0]
                 else:
                     if kwargs['templocal']:
-                        tempiostream.write(line+"\n");
-                        tempiostream.flush();
+                        tempiostream.write(line + "\n")
+                        tempiostream.flush()
                     if kwargs['writelocal']:
-                        outiolist[-1]+=line+"\n";
-                #if len(glob.glob(workpath+"/*.lock"))<kwargs['nworkers']:
-                #    lockfile=workpath+"/"+kwargs['stepid']+".lock";
-                #    with open(lockfile,'w') as lockstream:
-                #        lockstream.write(str(countallbatches));
-                #        lockstream.flush();
+                        outiolist[-1] += line + "\n"
+                #if len(glob.glob(workpath + "/*.lock")) < kwargs['nworkers']:
+                #    lockfile = workpath + "/" + kwargs['stepid'] + ".lock"
+                #    with open(lockfile, 'w') as lockstream:
+                #        lockstream.write(str(countallbatches))
+                #        lockstream.flush()
                 #    for bulkcoll in bulkdict.keys():
                 #        try:
-                #            bulkdict[bulkcoll].execute();
+                #            bulkdict[bulkcoll].execute()
                 #        except BulkWriteError as bwe:
-                #            pprint(bwe.details);
+                #            pprint(bwe.details)
                 #    while True:
                 #        try:
-                #            fcntl.flock(sys.stdout,fcntl.LOCK_EX | fcntl.LOCK_NB);
-                #            break;
+                #            fcntl.flock(sys.stdout, fcntl.LOCK_EX | fcntl.LOCK_NB)
+                #            break
                 #        except IOError:
-                #            sleep(0.01);
-                #    sys.stdout.write(tempiostream.getvalue());
-                #    sys.stdout.flush();
-                #    fcntl.flock(sys.stdout,fcntl.LOCK_UN);
-                #    bulkdict={};
-                #    tempiostream=cStringIO.StringIO();
-                #    countallbatches=0;
-                #    os.remove(lockfile);
-
-        if (len(bulkrequestslist)>1) and (len(glob.glob(workpath+"/*.lock"))<kwargs['nworkers']):
-            #if len(glob.glob(workpath+"/*.lock"))>=kwargs['nworkers']:
-            #    overlocked=True;
-            #    os.kill(process.pid,signal.SIGSTOP);
-            #    while len(glob.glob(workpath+"/*.lock"))>=kwargs['nworkers']:
-            #        sleep(0.01);
+                #            sleep(0.01)
+                #    sys.stdout.write(tempiostream.getvalue())
+                #    sys.stdout.flush()
+                #    fcntl.flock(sys.stdout, fcntl.LOCK_UN)
+                #    bulkdict = {}
+                #    tempiostream = cStringIO.StringIO()
+                #    countallbatches = 0
+                #    os.remove(lockfile)
+
+        if (len(bulkrequestslist) > 1) and (len(glob.glob(workpath + "/*.lock")) < kwargs['nworkers']):
+            #if len(glob.glob(workpath + "/*.lock")) >= kwargs['nworkers']:
+            #    overlocked = True
+            #    os.kill(process.pid, signal.SIGSTOP)
+            #    while len(glob.glob(workpath + "/*.lock")) >= kwargs['nworkers']:
+            #        sleep(0.01)
             #else:
-            #    overlocked=False;
-            lockfile=workpath+"/"+kwargs['stepid']+".lock";
-            with open(lockfile,'w') as lockstream:
-                lockstream.write("Writing "+str(countallbatches[0])+" items.");
-                lockstream.flush();
-            del countallbatches[0];
-            #print(bulkdict);
-            #sys.stdout.flush();
-            if dbtype=="mongodb":
-                for coll,requests in bulkrequestslist[0].items():
+            #    overlocked = False
+            lockfile = workpath + "/" + kwargs['stepid'] + ".lock"
+            with open(lockfile, 'w') as lockstream:
+                lockstream.write("Writing " + str(countallbatches[0]) + " items.")
+                lockstream.flush()
+            del countallbatches[0]
+            #print(bulkdict)
+            #sys.stdout.flush()
+            if dbtype == "mongodb":
+                for coll, requests in bulkrequestslist[0].items():
                     try:
-                        #bulkdict[bulkcoll].execute();
-                        bulkcolls[coll].bulk_write(requests,ordered=False);
+                        #bulkdict[bulkcoll].execute()
+                        bulkcolls[coll].bulk_write(requests, ordered = False)
                     except BulkWriteError as bwe:
-                        pprint(bwe.details);
-                del bulkrequestslist[0];
+                        pprint(bwe.details)
+                del bulkrequestslist[0]
             #while True:
             #    try:
-            #        fcntl.flock(sys.stdout,fcntl.LOCK_EX | fcntl.LOCK_NB);
-            #        break;
+            #        fcntl.flock(sys.stdout, fcntl.LOCK_EX | fcntl.LOCK_NB)
+            #        break
             #    except IOError:
-            #        sleep(0.01);
-            #tempiostream.close();
-            #with open(workpath+"/"+stepname+".temp","r") as tempiostream, open(workpath+"/"+stepname+".out","a") as iostream:
+            #        sleep(0.01)
+            #tempiostream.close()
+            #with open(workpath + "/" + stepname + ".temp", "r") as tempiostream, open(workpath + "/" + stepname + ".out", "a") as iostream:
             #    for line in tempiostream:
-            #        iostream.write(line);
-            #        iostream.flush();
-            #    os.remove(tempiostream.name);
-            #sys.stdout.write(tempiostream.getvalue());
-            #sys.stdout.flush();
+            #        iostream.write(line)
+            #        iostream.flush()
+            #    os.remove(tempiostream.name)
+            #sys.stdout.write(tempiostream.getvalue())
+            #sys.stdout.flush()
             if kwargs['logging']:
-                sys.stdout.write(logiolist[0]);
-                sys.stdout.flush();
-                del logiolist[0];
+                #print(len(logiolist[0].rstrip("\n").split("\n")))
+                sys.stdout.flush()
+                logiotime = ""
+                for logio in logiolist[0].rstrip("\n").split("\n"):
+                    logiotime += datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S") + " UTC: " + logio + "\n"
+                sys.stdout.write(logiotime)
+                sys.stdout.flush()
+                del logiolist[0]
             if kwargs['templocal']:
-                name=tempiostream.name;
-                tempiostream.close();
-                os.remove(name);
-                tempiostream=open(name,"w");
+                name = tempiostream.name
+                tempiostream.close()
+                os.remove(name)
+                tempiostream = open(name, "w")
             if kwargs['writelocal'] or kwargs['statslocal']:
-                outiostream.write(outiolist[0]);
-                outiostream.flush();
-                del outiolist[0];
-            #fcntl.flock(sys.stdout,fcntl.LOCK_UN);
-            #bulkdict={};
-            #tempiostream=open(workpath+"/"+stepname+".temp","w");
-            #countallbatches=0;
-            os.remove(lockfile);
+                outiostream.write(outiolist[0])
+                outiostream.flush()
+                del outiolist[0]
+            #fcntl.flock(sys.stdout, fcntl.LOCK_UN)
+            #bulkdict = {}
+            #tempiostream = open(workpath + "/" + stepname + ".temp", "w")
+            #countallbatches = 0
+            os.remove(lockfile)
             #if overlocked:
-            #    os.kill(process.pid,signal.SIGCONT);
+            #    os.kill(process.pid, signal.SIGCONT)
 
     while not stderr_queue.empty():
-        stderr_line=stderr_queue.get().rstrip("\n");
+        stderr_line = stderr_queue.get().rstrip("\n")
         if stderr_line not in ignoredstrings:
-            if kwargs['controllername']!=None:
-                exitcode=get_exitcode(kwargs['stepid']);
-            with open(filename+".err","a") as errstream:
-                if kwargs['controllername']!=None:
-                    errstream.write("ExitCode: "+exitcode+"\n");
-                errstream.write(stderr_line+"\n")
-                errstream.flush();
+            if kwargs['controllername'] != None:
+                exitcode = get_exitcode(kwargs['stepid'])
+            with open(filename + ".err", "a") as errstream:
+                if kwargs['controllername'] != None:
+                    errstream.write("ExitCode: " + exitcode + "\n")
+                errstream.write(stderr_line + "\n")
+                errstream.flush()
             #while True:
             #    try:
-            #        fcntl.flock(sys.stderr,fcntl.LOCK_EX | fcntl.LOCK_NB);
-            #        break;
+            #        fcntl.flock(sys.stderr, fcntl.LOCK_EX | fcntl.LOCK_NB)
+            #        break
             #    except IOError:
-            #        sleep(0.01);
-            sys.stderr.write(stderr_line+"\n");
-            sys.stderr.flush();
-            #fcntl.flock(sys.stderr,fcntl.LOCK_UN);
+            #        sleep(0.01)
+            sys.stderr.write(stderr_line + "\n")
+            sys.stderr.flush()
+            #fcntl.flock(sys.stderr, fcntl.LOCK_UN)
 
-    sleep(kwargs['delay']);
+    sleep(kwargs['delay'])
 
 while not stdout_queue.empty():
-    while (not stdout_queue.empty()) and ((len(bulkrequestslist)<=1 and countthisbatch<nbatch) or (len(glob.glob(workpath+"/*.lock"))>=kwargs['nworkers'])):
-        line=stdout_queue.get().rstrip("\n");
+    while (not stdout_queue.empty()) and ((len(bulkrequestslist) <= 1 and countthisbatch < nbatch) or (len(glob.glob(workpath + "/*.lock")) >= kwargs['nworkers'])):
+        line = stdout_queue.get().rstrip("\n")
         if line not in ignoredstrings:
-            linehead=re.sub("^([-+&@].*?>|None).*",r"\1",line);
-            linemarker=linehead[0];
-            if linemarker=="-":
-                newcollection,strindexdoc=linehead[1:-1].split(".");
-                newindexdoc=json.loads(strindexdoc);
-                linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
-                doc=json.loads(linedoc);
-                if dbtype=="mongodb":
-                    bsonsize-=get_bsonsize(doc);
+            linehead = re.sub("^([- + &@].*?>|None).*", r"\1", line)
+            linemarker = linehead[0]
+            if linemarker == "-":
+                newcollection, strindexdoc = linehead[1:-1].split(".")
+                newindexdoc = json.loads(strindexdoc)
+                linedoc = re.sub("^[-+&@].*?>", "", line).rstrip("\n")
+                doc = json.loads(linedoc)
+                if dbtype == "mongodb":
+                    bsonsize -= get_bsonsize(doc)
                 if kwargs['templocal']:
-                    tempiostream.write(line+"\n");
-                    tempiostream.flush();
+                    tempiostream.write(line + "\n")
+                    tempiostream.flush()
                 if kwargs['writelocal']:
-                    outiolist[-1]+=line+"\n";
+                    outiolist[-1] += line + "\n"
                 if kwargs['writedb']:
                     if newcollection not in bulkcolls.keys():
-                        #bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
-                        if dbtype=="mongodb":
-                            bulkcolls[newcollection]=db.get_collection(newcollection,write_concern=WriteConcern(fsync=True));
+                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
+                        if dbtype == "mongodb":
+                            bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                     if newcollection not in bulkrequestslist[-1].keys():
-                        bulkrequestslist[-1][newcollection]=[];
-                    #bulkdict[newcollection].find(newindexdoc).update({"$unset":doc});
-                    if dbtype=="mongodb":
-                        bulkrequestslist[-1][newcollection]+=[UpdateOne(newindexdoc,{"$unset":doc})];
-            elif linemarker=="+":
-                #tempiostream.write(linehead[1:-1].split(".")+"\n");
-                #sys.stdout.flush();
-                newcollection,strindexdoc=linehead[1:-1].split(".");
-                newindexdoc=json.loads(strindexdoc);
-                linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
-                doc=json.loads(linedoc);
-                if dbtype=="mongodb":
-                    bsonsize+=get_bsonsize(doc);
+                        bulkrequestslist[-1][newcollection] = []
+                    #bulkdict[newcollection].find(newindexdoc).update({"$unset": doc})
+                    if dbtype == "mongodb":
+                        bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$unset": doc})]
+            elif linemarker == " + ":
+                #tempiostream.write(linehead[1:-1].split(".") + "\n")
+                #sys.stdout.flush()
+                newcollection, strindexdoc = linehead[1:-1].split(".")
+                newindexdoc = json.loads(strindexdoc)
+                linedoc = re.sub("^[-+&@].*?>", "", line).rstrip("\n")
+                doc = json.loads(linedoc)
+                if dbtype == "mongodb":
+                    bsonsize += get_bsonsize(doc)
                 if kwargs['templocal']:
-                    tempiostream.write(line+"\n");
-                    tempiostream.flush();
+                    tempiostream.write(line + "\n")
+                    tempiostream.flush()
                 if kwargs['writelocal']:
-                    outiolist[-1]+=line+"\n";
+                    outiolist[-1] += line + "\n"
                 if kwargs['writedb']:
                     if newcollection not in bulkcolls.keys():
-                        #bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
-                        if dbtype=="mongodb":
-                            bulkcolls[newcollection]=db.get_collection(newcollection,write_concern=WriteConcern(fsync=True));
+                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
+                        if dbtype == "mongodb":
+                            bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                     if newcollection not in bulkrequestslist[-1].keys():
-                        bulkrequestslist[-1][newcollection]=[];
-                    #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set":doc});
-                    if dbtype=="mongodb":
-                        bulkrequestslist[-1][newcollection]+=[UpdateOne(newindexdoc,{"$set":doc},upsert=True)];
-            elif linemarker=="&":
-                newcollection,strindexdoc=linehead[1:-1].split(".");
-                newindexdoc=json.loads(strindexdoc);
-                linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
-                doc=json.loads(linedoc);
-                if dbtype=="mongodb":
-                    bsonsize+=get_bsonsize(doc);
+                        bulkrequestslist[-1][newcollection] = []
+                    #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set": doc})
+                    if dbtype == "mongodb":
+                        bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$set": doc}, upsert = True)]
+            elif linemarker == "&":
+                newcollection, strindexdoc = linehead[1:-1].split(".")
+                newindexdoc = json.loads(strindexdoc)
+                linedoc = re.sub("^[-+&@].*?>", "", line).rstrip("\n")
+                doc = json.loads(linedoc)
+                if dbtype == "mongodb":
+                    bsonsize += get_bsonsize(doc)
                 if kwargs['templocal']:
-                    tempiostream.write(line+"\n");
-                    tempiostream.flush();
+                    tempiostream.write(line + "\n")
+                    tempiostream.flush()
                 if kwargs['writelocal']:
-                    outiolist[-1]+=line+"\n";
+                    outiolist[-1] += line + "\n"
                 if kwargs['writedb']:
                     if newcollection not in bulkcolls.keys():
-                        #bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
-                        if dbtype=="mongodb":
-                            bulkcolls[newcollection]=db.get_collection(newcollection,write_concern=WriteConcern(fsync=True));
+                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
+                        if dbtype == "mongodb":
+                            bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                     if newcollection not in bulkrequestslist[-1].keys():
-                        bulkrequestslist[-1][newcollection]=[];
-                    #bulkdict[newcollection].find(newindexdoc).upsert().update({"$addToSet":doc});
-                    if dbtype=="mongodb":
-                        bulkrequestslist[-1][newcollection]+=[UpdateOne(newindexdoc,{"$addToSet":doc},upsert=True)];
-            elif linemarker=="@":
+                        bulkrequestslist[-1][newcollection] = []
+                    #bulkdict[newcollection].find(newindexdoc).upsert().update({"$addToSet": doc})
+                    if dbtype == "mongodb":
+                        bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$addToSet": doc}, upsert = True)]
+            elif linemarker == "@":
                 if kwargs['statslocal'] or kwargs['statsdb']:
-                    cputime="%.2f" % stats_reader.stat("TotalCPUTime");
-                    maxrss=stats_reader.max_stat("Rss");
-                    maxvmsize=stats_reader.max_stat("Size");
-                #    stats=getstats("sstat",["MaxRSS","MaxVMSize"],kwargs['stepid']);
-                #    if (len(stats)==1) and (stats[0]==""):
-                #        newtotcputime,maxrss,maxvmsize=[eval(x) for x in getstats("sacct",["CPUTimeRAW","MaxRSS","MaxVMSize"],kwargs['stepid'])];
+                    cputime = "%.2f" % stats_reader.stat("TotalCPUTime")
+                    maxrss = stats_reader.max_stat("Rss")
+                    maxvmsize = stats_reader.max_stat("Size")
+                #    stats = getstats("sstat", ["MaxRSS", "MaxVMSize"], kwargs['stepid'])
+                #    if (len(stats) == 1) and (stats[0] == ""):
+                #        newtotcputime, maxrss, maxvmsize = [eval(x) for x in getstats("sacct", ["CPUTimeRAW", "MaxRSS", "MaxVMSize"], kwargs['stepid'])]
                 #    else:
-                #        newtotcputime=eval(getstats("sacct",["CPUTimeRAW"],kwargs['stepid'])[0]);
-                #        maxrss,maxvmsize=stats;
-                #    cputime=newtotcputime-totcputime;
-                #    totcputime=newtotcputime;
-                #newcollection,strindexdoc=linehead[1:].split("<")[0].split(".");
-                #newindexdoc=json.loads(strindexdoc);
-                newcollection=basecoll;
-                doc=json.loads(temp_queue.get());
-                newindexdoc=dict([(x,doc[x]) for x in kwargs['dbindexes']]);               
+                #        newtotcputime = eval(getstats("sacct", ["CPUTimeRAW"], kwargs['stepid'])[0])
+                #        maxrss, maxvmsize = stats
+                #    cputime = newtotcputime-totcputime
+                #    totcputime = newtotcputime
+                #newcollection, strindexdoc = linehead[1:].split("<")[0].split(".")
+                #newindexdoc = json.loads(strindexdoc)
+                newcollection = basecoll
+                doc = json.loads(temp_queue.get())
+                newindexdoc = dict([(x, doc[x]) for x in kwargs['dbindexes']]);               
                 if kwargs['logging']:
-                    logiolist[-1]+=line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"<OUT\n";
-                    sys.stdout.write(line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"<TEMP\n");
-                    sys.stdout.flush();
+                    logiolist[-1] += line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "<OUT\n"
+                    sys.stdout.write(datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S") + " UTC: " + line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "<TEMP\n")
+                    sys.stdout.flush()
                 if kwargs['templocal']:
-                    tempiostream.write(line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"\n");
-                    tempiostream.write("CPUTime: "+str(cputime)+" seconds\n");
-                    tempiostream.write("MaxRSS: "+str(maxrss)+" bytes\n");
-                    tempiostream.write("MaxVMSize: "+str(maxvmsize)+" bytes\n");
-                    tempiostream.write("BSONSize: "+str(bsonsize)+" bytes\n");
-                    tempiostream.flush();
+                    tempiostream.write(line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "\n")
+                    tempiostream.write("CPUTime: " + str(cputime) + " seconds\n")
+                    tempiostream.write("MaxRSS: " + str(maxrss) + " bytes\n")
+                    tempiostream.write("MaxVMSize: " + str(maxvmsize) + " bytes\n")
+                    tempiostream.write("BSONSize: " + str(bsonsize) + " bytes\n")
+                    tempiostream.flush()
                 if kwargs['writelocal']:
-                    outiolist[-1]+=line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"\n";
+                    outiolist[-1] += line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "\n"
                 if kwargs['statslocal']:
-                    outiolist[-1]+="CPUTime: "+str(cputime)+" seconds\n";
-                    outiolist[-1]+="MaxRSS: "+str(maxrss)+" bytes\n";
-                    outiolist[-1]+="MaxVMSize: "+str(maxvmsize)+" bytes\n";
-                    outiolist[-1]+="BSONSize: "+str(bsonsize)+" bytes\n";
-                statsmark={};
+                    outiolist[-1] += "CPUTime: " + str(cputime) + " seconds\n"
+                    outiolist[-1] += "MaxRSS: " + str(maxrss) + " bytes\n"
+                    outiolist[-1] += "MaxVMSize: " + str(maxvmsize) + " bytes\n"
+                    outiolist[-1] += "BSONSize: " + str(bsonsize) + " bytes\n"
+                statsmark = {}
                 if kwargs['statsdb']:
-                    statsmark.update({modname+"STATS":{"CPUTIME":cputime,"MAXRSS":maxrss,"MAXVMSIZE":maxvmsize,"BSONSIZE":bsonsize}});
-                if kwargs['markdone']!="":
-                    statsmark.update({modname+kwargs['markdone']:True});
-                if len(statsmark)>0:
+                    statsmark.update({modname + "STATS": {"CPUTIME": cputime, "MAXRSS": maxrss, "MAXVMSIZE": maxvmsize, "BSONSIZE": bsonsize}})
+                if kwargs['markdone'] != "":
+                    statsmark.update({modname + kwargs['markdone']: True})
+                if len(statsmark) > 0:
                     if newcollection not in bulkcolls.keys():
-                        #bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
-                        if dbtype=="mongodb":
-                            bulkcolls[newcollection]=db.get_collection(newcollection,write_concern=WriteConcern(fsync=True));
+                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
+                        if dbtype == "mongodb":
+                            bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                     if newcollection not in bulkrequestslist[-1].keys():
-                        bulkrequestslist[-1][newcollection]=[];
-                    #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set":statsmark});
-                    if dbtype=="mongodb":
-                        bulkrequestslist[-1][newcollection]+=[UpdateOne(newindexdoc,{"$set":statsmark},upsert=True)];
-                bsonsize=0;
-                countthisbatch+=1;
-                countallbatches[-1]+=1;
-                if countthisbatch==nbatch:
-                    bulkrequestslist+=[{}];
+                        bulkrequestslist[-1][newcollection] = []
+                    #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set": statsmark})
+                    if dbtype == "mongodb":
+                        bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$set": statsmark}, upsert = True)]
+                bsonsize = 0
+                countthisbatch += 1
+                countallbatches[-1] += 1
+                if countthisbatch == nbatch:
+                    bulkrequestslist += [{}]
                     if kwargs['logging']:
-                        logiolist+=[""];
+                        logiolist += [""]
                     if kwargs['writelocal'] or kwargs['statslocal']:
-                        outiolist+=[""];
-                    countthisbatch=0;
-                    nbatch=randint(1,kwargs['nbatch']) if kwargs['random_nbatch'] else kwargs['nbatch'];
-                    countallbatches+=[0];
+                        outiolist += [""]
+                    countthisbatch = 0
+                    nbatch = randint(1, kwargs['nbatch']) if kwargs['random_nbatch'] else kwargs['nbatch']
+                    countallbatches += [0]
             else:
                 if kwargs['templocal']:
-                    tempiostream.write(line+"\n");
-                    tempiostream.flush();
+                    tempiostream.write(line + "\n")
+                    tempiostream.flush()
                 if kwargs['writelocal']:
-                    outiolist[-1]+=line+"\n";
-            #if len(glob.glob(workpath+"/*.lock"))<kwargs['nworkers']:
-            #    lockfile=workpath+"/"+kwargs['stepid']+".lock";
-            #    with open(lockfile,'w') as lockstream:
-            #        lockstream.write(str(countallbatches));
-            #        lockstream.flush();
+                    outiolist[-1] += line + "\n"
+            #if len(glob.glob(workpath + "/*.lock")) < kwargs['nworkers']:
+            #    lockfile = workpath + "/" + kwargs['stepid'] + ".lock"
+            #    with open(lockfile, 'w') as lockstream:
+            #        lockstream.write(str(countallbatches))
+            #        lockstream.flush()
             #    for bulkcoll in bulkdict.keys():
             #        try:
-            #            bulkdict[bulkcoll].execute();
+            #            bulkdict[bulkcoll].execute()
             #        except BulkWriteError as bwe:
-            #            pprint(bwe.details);
+            #            pprint(bwe.details)
             #    while True:
             #        try:
-            #            fcntl.flock(sys.stdout,fcntl.LOCK_EX | fcntl.LOCK_NB);
-            #            break;
+            #            fcntl.flock(sys.stdout, fcntl.LOCK_EX | fcntl.LOCK_NB)
+            #            break
             #        except IOError:
-            #            sleep(0.01);
-            #    sys.stdout.write(tempiostream.getvalue());
-            #    sys.stdout.flush();
-            #    fcntl.flock(sys.stdout,fcntl.LOCK_UN);
-            #    bulkdict={};
-            #    tempiostream=cStringIO.StringIO();
-            #    countallbatches=0;
-            #    os.remove(lockfile);
-
-    if (len(bulkrequestslist)>1) and (len(glob.glob(workpath+"/*.lock"))<kwargs['nworkers']):
-        #if len(glob.glob(workpath+"/*.lock"))>=kwargs['nworkers']:
-        #    overlocked=True;
-        #    os.kill(process.pid,signal.SIGSTOP);
-        #    while len(glob.glob(workpath+"/*.lock"))>=kwargs['nworkers']:
-        #        sleep(0.01);
+            #            sleep(0.01)
+            #    sys.stdout.write(tempiostream.getvalue())
+            #    sys.stdout.flush()
+            #    fcntl.flock(sys.stdout, fcntl.LOCK_UN)
+            #    bulkdict = {}
+            #    tempiostream = cStringIO.StringIO()
+            #    countallbatches = 0
+            #    os.remove(lockfile)
+
+    if (len(bulkrequestslist) > 1) and (len(glob.glob(workpath + "/*.lock")) < kwargs['nworkers']):
+        #if len(glob.glob(workpath + "/*.lock")) >= kwargs['nworkers']:
+        #    overlocked = True
+        #    os.kill(process.pid, signal.SIGSTOP)
+        #    while len(glob.glob(workpath + "/*.lock")) >= kwargs['nworkers']:
+        #        sleep(0.01)
         #else:
-        #    overlocked=False;
-        lockfile=workpath+"/"+kwargs['stepid']+".lock";
-        with open(lockfile,'w') as lockstream:
-            lockstream.write("Writing "+str(countallbatches[0])+" items.");
-            lockstream.flush();
-        del countallbatches[0];
-        #print(bulkdict);
-        #sys.stdout.flush();
-        if dbtype=="mongodb":
-            for coll,requests in bulkrequestslist[0].items():
+        #    overlocked = False
+        lockfile = workpath + "/" + kwargs['stepid'] + ".lock"
+        with open(lockfile, 'w') as lockstream:
+            lockstream.write("Writing " + str(countallbatches[0]) + " items.")
+            lockstream.flush()
+        del countallbatches[0]
+        #print(bulkdict)
+        #sys.stdout.flush()
+        if dbtype == "mongodb":
+            for coll, requests in bulkrequestslist[0].items():
                 try:
-                    #bulkdict[bulkcoll].execute();
-                    bulkcolls[coll].bulk_write(requests,ordered=False);
+                    #bulkdict[bulkcoll].execute()
+                    bulkcolls[coll].bulk_write(requests, ordered = False)
                 except BulkWriteError as bwe:
-                    pprint(bwe.details);
-            del bulkrequestslist[0];
+                    pprint(bwe.details)
+            del bulkrequestslist[0]
         #while True:
         #    try:
-        #        fcntl.flock(sys.stdout,fcntl.LOCK_EX | fcntl.LOCK_NB);
-        #        break;
+        #        fcntl.flock(sys.stdout, fcntl.LOCK_EX | fcntl.LOCK_NB)
+        #        break
         #    except IOError:
-        #        sleep(0.01);
-        #tempiostream.close();
-        #with open(workpath+"/"+stepname+".temp","r") as tempiostream, open(workpath+"/"+stepname+".out","a") as iostream:
+        #        sleep(0.01)
+        #tempiostream.close()
+        #with open(workpath + "/" + stepname + ".temp", "r") as tempiostream, open(workpath + "/" + stepname + ".out", "a") as iostream:
         #    for line in tempiostream:
-        #        iostream.write(line);
-        #        iostream.flush();
-        #    os.remove(tempiostream.name);
-        #sys.stdout.write(tempiostream.getvalue());
-        #sys.stdout.flush();
+        #        iostream.write(line)
+        #        iostream.flush()
+        #    os.remove(tempiostream.name)
+        #sys.stdout.write(tempiostream.getvalue())
+        #sys.stdout.flush()
         if kwargs['logging']:
-            sys.stdout.write(logiolist[0]);
-            sys.stdout.flush();
-            del logiolist[0];
+            #print(len(logiolist[0].rstrip("\n").split("\n")))
+            sys.stdout.flush()
+            logiotime = ""
+            for logio in logiolist[0].rstrip("\n").split("\n"):
+                logiotime += datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S") + " UTC: " + logio + "\n"
+            sys.stdout.write(logiotime)
+            sys.stdout.flush()
+            del logiolist[0]
         if kwargs['templocal']:
-            name=tempiostream.name;
-            tempiostream.close();
-            os.remove(name);
-            tempiostream=open(name,"w");
+            name = tempiostream.name
+            tempiostream.close()
+            os.remove(name)
+            tempiostream = open(name, "w")
         if kwargs['writelocal'] or kwargs['statslocal']:
-            outiostream.write(outiolist[0]);
-            outiostream.flush();
-            del outiolist[0];
-        #fcntl.flock(sys.stdout,fcntl.LOCK_UN);
-        #bulkdict={};
-        #tempiostream=open(workpath+"/"+stepname+".temp","w");
-        #countallbatches=0;
-        os.remove(lockfile);
+            outiostream.write(outiolist[0])
+            outiostream.flush()
+            del outiolist[0]
+        #fcntl.flock(sys.stdout, fcntl.LOCK_UN)
+        #bulkdict = {}
+        #tempiostream = open(workpath + "/" + stepname + ".temp", "w")
+        #countallbatches = 0
+        os.remove(lockfile)
         #if overlocked:
-        #    os.kill(process.pid,signal.SIGCONT);
+        #    os.kill(process.pid, signal.SIGCONT)
 
 while not stderr_queue.empty():
-    stderr_line=stderr_queue.get().rstrip("\n");
+    stderr_line = stderr_queue.get().rstrip("\n")
     if stderr_line not in ignoredstrings:
-        if kwargs['controllername']!=None:
-            exitcode=get_exitcode(kwargs['stepid']);
-        with open(filename+".err","a") as errstream:
-            if kwargs['controllername']!=None:
-                errstream.write("ExitCode: "+exitcode+"\n");
-            errstream.write(stderr_line+"\n")
-            errstream.flush();
+        if kwargs['controllername'] != None:
+            exitcode = get_exitcode(kwargs['stepid'])
+        with open(filename + ".err", "a") as errstream:
+            if kwargs['controllername'] != None:
+                errstream.write("ExitCode: " + exitcode + "\n")
+            errstream.write(stderr_line + "\n")
+            errstream.flush()
         #while True:
         #    try:
-        #        fcntl.flock(sys.stderr,fcntl.LOCK_EX | fcntl.LOCK_NB);
-        #        break;
+        #        fcntl.flock(sys.stderr, fcntl.LOCK_EX | fcntl.LOCK_NB)
+        #        break
         #    except IOError:
-        #        sleep(0.01);
-        sys.stderr.write(stderr_line+"\n");
-        sys.stderr.flush();
-        #fcntl.flock(sys.stderr,fcntl.LOCK_UN);
-
-while len(bulkrequestslist)>0:
-    while len(glob.glob(workpath+"/*.lock"))>=kwargs['nworkers']:
-        sleep(kwargs['delay']);
-
-    if (len(bulkrequestslist)>0) and (len(glob.glob(workpath+"/*.lock"))<kwargs['nworkers']):
-        lockfile=workpath+"/"+kwargs['stepid']+".lock";
-        with open(lockfile,'w') as lockstream:
-            lockstream.write("Writing "+str(countallbatches[0])+" items.");
-            lockstream.flush();
-        del countallbatches[0];
-
-        for coll,requests in bulkrequestslist[0].items():
+        #        sleep(0.01)
+        sys.stderr.write(stderr_line + "\n")
+        sys.stderr.flush()
+        #fcntl.flock(sys.stderr, fcntl.LOCK_UN)
+
+while len(bulkrequestslist) > 0:
+    while len(glob.glob(workpath + "/*.lock")) >= kwargs['nworkers']:
+        sleep(kwargs['delay'])
+
+    if (len(bulkrequestslist) > 0) and (len(glob.glob(workpath + "/*.lock")) < kwargs['nworkers']):
+        lockfile = workpath + "/" + kwargs['stepid'] + ".lock"
+        with open(lockfile, 'w') as lockstream:
+            lockstream.write("Writing " + str(countallbatches[0]) + " items.")
+            lockstream.flush()
+        del countallbatches[0]
+
+        for coll, requests in bulkrequestslist[0].items():
             try:
-                bulkcolls[coll].bulk_write(requests,ordered=False);
+                bulkcolls[coll].bulk_write(requests, ordered = False)
             except BulkWriteError as bwe:
-                pprint(bwe.details);
-        del bulkrequestslist[0];
+                pprint(bwe.details)
+        del bulkrequestslist[0]
 
         if kwargs['logging']:
-            sys.stdout.write(logiolist[0]);
-            sys.stdout.flush();
-            del logiolist[0];
+            #print(len(logiolist[0].rstrip("\n").split("\n")))
+            sys.stdout.flush()
+            logiotime = ""
+            for logio in logiolist[0].rstrip("\n").split("\n"):
+                logiotime += datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S") + " UTC: " + logio + "\n"
+            sys.stdout.write(logiotime)
+            sys.stdout.flush()
+            del logiolist[0]
         if kwargs['templocal']:
-            name=tempiostream.name;
-            tempiostream.close();
-            os.remove(name);
-            tempiostream=open(name,"w");
+            name = tempiostream.name
+            tempiostream.close()
+            os.remove(name)
+            tempiostream = open(name, "w")
         if kwargs['writelocal'] or kwargs['statslocal']:
-            outiostream.write(outiolist[0]);
-            outiostream.flush();
-            del outiolist[0];
+            outiostream.write(outiolist[0])
+            outiostream.flush()
+            del outiolist[0]
 
-        os.remove(lockfile);
+        os.remove(lockfile)
 
-if kwargs['input_file']!=None:
-    stdin_iter_file.close();
+if kwargs['input_file'] != None:
+    stdin_iter_file.close()
 
 if kwargs['templocal']:
     if not tempiostream.closed:
-        tempiostream.close();
+        tempiostream.close()
     if os.path.exists(tempiostream.name):
-        os.remove(tempiostream.name);
+        os.remove(tempiostream.name)
 if kwargs['writelocal'] or kwargs['statslocal']:
-    outiostream.close();
+    outiostream.close()
 
-stdout_reader.join();
-stderr_reader.join();
+stdout_reader.join()
+stderr_reader.join()
 if kwargs['statslocal'] or kwargs['statsdb']:
-    stats_reader.join();
+    stats_reader.join()
 
-process.stdin.close();
-process.stdout.close();
-process.stderr.close();
+process.stdin.close()
+process.stdout.close()
+process.stderr.close()
 
-if dbtype=="mongodb":
-    dbclient.close();
\ No newline at end of file
+if dbtype == "mongodb":
+    dbclient.close()
\ No newline at end of file
diff --git a/install b/install
index b857948..fa743b1 100755
--- a/install
+++ b/install
@@ -1,7 +1,9 @@
 #!/bin/bash
 
-USER_LOCAL="\${HOME}/opt"
-CRUNCH_ROOT="$(cd $(dirname ${BASH_SOURCE[0]}); pwd -P)"
+# Accept user-specified arguments
+
+USER_LOCAL="${HOME}/opt"
+CRUNCH_ROOT="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd -P)"
 
 SHORTOPTS="u:c:" 
 LONGOPTS="USER_LOCAL:,CRUNCH_ROOT:" 
@@ -33,92 +35,85 @@ do
     shift
 done
 
-USER_LOCAL="${USER_LOCAL}"
-pythonpath="${CRUNCH_ROOT}/packages/python"
-softwarepath="${CRUNCH_ROOT}/modules/install"
-currpath=$(pwd)
+# Source DBCrunch environment variables
 
-if ! [ -f ${HOME}/.bashrc ]
+echo "Sourcing DBCrunch environment variables..."
+
+if [ ! -f ${HOME}/.bashrc ]
 then
     touch ${HOME}/.bashrc
 fi
 
-rm ${HOME}/.newbashrc 2>/dev/null
-firstline=true
-prevline=""
-while IFS=$'\n' read -r line
-do
-    if ! ${firstline} && [[ "${prevline}" == "" ]] && ! [[ "${line}" =~ .*"# DBCrunch environment variables".* ]]
-    then
-        printf '%s\n' "${prevline}" >> ${HOME}/.newbashrc
-    fi
+if ! grep -q "# Source DBCrunch environment variables" ${HOME}/.bashrc
+then
+    echo "" >> ${HOME}/.bashrc
+    echo "# Source DBCrunch environment variables" >> ${HOME}/.bashrc
+    echo "" >> ${HOME}/.bashrc
+    echo "export USER_LOCAL="$(echo ${USER_LOCAL} | sed 's|'"${HOME}"'|${HOME}|g' | sed 's|'"${USER}"'|${USER}|g') >> ${HOME}/.bashrc
+    echo "export CRUNCH_ROOT="$(echo ${CRUNCH_ROOT} | sed 's|'"${HOME}"'|${HOME}|g' | sed 's|'"${USER}"'|${USER}|g') >> ${HOME}/.bashrc
+    echo "export PATH=\${USER_LOCAL}/bin:\${CRUNCH_ROOT}/bin:\${PATH}" >> ${HOME}/.bashrc
+fi
 
-    if [[ "${line}" != "" ]] && ! [[ "${line}" =~ .*"# DBCrunch environment variables".* ]] && ! [[ "${line}" =~ .*"export USER_LOCAL".* ]] && ! [[ "${line}" =~ .*"export CRUNCH_ROOT".* ]] && ! [[ "${line}" =~ .*"export PATH".*"CRUNCH_ROOT".* ]]
-    then
-        printf '%s\n' "${line}" >> ${HOME}/.newbashrc
-    fi
-    firstline=false
-    prevline=${line}
-done < ${HOME}/.bashrc
+# Source DBCrunch bash completions
+
+echo "Sourcing DBCrunch bash completions..."
+
+if [ ! -f ${HOME}/.bash_completion ]
+then
+    touch ${HOME}/.bash_completion
+fi
 
-echo "" >> ${HOME}/.newbashrc
-echo "# DBCrunch environment variables" >> ${HOME}/.newbashrc
-echo "export USER_LOCAL=${USER_LOCAL}" >> ${HOME}/.newbashrc
-echo "export CRUNCH_ROOT="$(echo ${CRUNCH_ROOT} | sed 's|'"${HOME}"'|${HOME}|g' | sed 's|'"${USER}"'|${USER}|g') >> ${HOME}/.newbashrc
-echo "export PATH=\${USER_LOCAL}/bin:\${CRUNCH_ROOT}/bin:\${PATH}" >> ${HOME}/.newbashrc
+if ! grep -q "# Source DBCrunch bash completions" ${HOME}/.bash_completion
+then
+    echo "" >> ${HOME}/.bash_completion
+    echo "# Source DBCrunch bash completions" >> ${HOME}/.bash_completion
+    echo "" >> ${HOME}/.bash_completion
+    echo "if [ -f \${CRUNCH_ROOT}/bin/bash_completion ]" >> ${HOME}/.bash_completion
+    echo "then" >> ${HOME}/.bash_completion
+    echo "    . \${CRUNCH_ROOT}/bin/bash_completion" >> ${HOME}/.bash_completion
+    echo "fi" >> ${HOME}/.bash_completion
+fi
 
-mv ${HOME}/.bashrc ${HOME}/.oldbashrc
-mv ${HOME}/.newbashrc ${HOME}/.bashrc
+# Source all bash completions
 
-wait
-source ${HOME}/.bashrc
-wait
+echo "Sourcing all bash completions..."
 
-if [ ! -d "${HOME}/.bash_completion" ]
+if ! grep -q "# Source bash completions" ${HOME}/.bashrc
 then
-    touch ${HOME}/.bash_completion
+    echo "" >> ${HOME}/.bashrc
+    echo "# Source bash completions" >> ${HOME}/.bashrc
+    echo "" >> ${HOME}/.bashrc
+    echo "if [ -f \${HOME}/.bash_completion ]" >> ${HOME}/.bashrc
+    echo "then" >> ${HOME}/.bashrc
+    echo "    . \${HOME}/.bash_completion" >> ${HOME}/.bashrc
+    echo "fi" >> ${HOME}/.bashrc
 fi
-rm ${HOME}/.newbash_completion 2>/dev/null
-firstline=true
-prevline=""
-while IFS=$'\n' read -r line
-do
-    if ! ${firstline} && [[ "${prevline}" == "" ]] && ! [[ "${line}" =~ .*"# DBCrunch completions".* ]]
-    then
-        printf '%s\n' "${prevline}" >> ${HOME}/.newbash_completion
-    fi
 
-    if [[ "${line}" != "" ]] && ! [[ "${line}" =~ .*"# DBCrunch completions".* ]] && ! [[ "${line}" =~ .*". ${CRUNCH_ROOT}/bin/bash_completion".* ]]
-    then
-        printf '%s\n' "${line}" >> ${HOME}/.newbash_completion
-    fi
-    firstline=false
-    prevline=${line}
-done < ${HOME}/.bash_completion
+. ${HOME}/.bashrc
 
-echo "" >> ${HOME}/.newbash_completion
-echo "# DBCrunch completions" >> ${HOME}/.newbash_completion
-echo ". ${CRUNCH_ROOT}/bin/bash_completion" >> ${HOME}/.newbash_completion
+currpath=$(pwd)
 
-mv ${HOME}/.bash_completion ${HOME}/.oldbash_completion
-mv ${HOME}/.newbash_completion ${HOME}/.bash_completion
+# Update submodules
 
-wait
-source ${HOME}/.bash_completion
-wait
+echo "Updating submodules..."
 
-#Update submodules
 cd ${CRUNCH_ROOT}
 git submodule update --init --recursive
 
-#Install external software
-for file in $(find ${softwarepath}/ -maxdepth 1 -type f 2>/dev/null)
+# Install external software
+
+echo "Installing external software..."
+
+for file in $(find ${CRUNCH_ROOT}/modules/install -mindepth 1 -maxdepth 1 -type f 2>/dev/null)
 do
     ${file}
     wait
 done
 
-#Install mpi4py
+#Install mpi4py package
+
+echo "Installing mpi4py package..."
+
 if $(python -c "import mpi4py" |& grep -q "ImportError" && echo true || echo false)
 then
     cd ${HOME}
@@ -139,6 +134,10 @@ then
     rm -rf pymongo
 fi
 
+#Install database APIs
+
+echo "Installing database APIs..."
+
 #Install pymongo
 if $(python -c "import pymongo" |& grep -q "ImportError" && echo true || echo false)
 then
@@ -160,16 +159,20 @@ then
     rm -rf pymongo
 fi
 
-#Install mongolink
-cd ${pythonpath}/mongolink
+#Install mongojoin
+cd ${CRUNCH_ROOT}/api/db/mongojoin
 python setup.py install --user --record filespy.txt
 if [[ "$(command -v sage)" != "" ]]
 then
     sage --python setup.py install --user --record filessage.txt
 fi
 
-#Install slurmlink
-cd ${pythonpath}/slurmlink
+#Install workload APIs
+
+echo "Installing workload APIs..."
+
+#Install crunch_slurm
+cd ${CRUNCH_ROOT}/api/workload/crunch_slurm
 python setup.py install --user --record filespy.txt
 if [[ "$(command -v sage)" != "" ]]
 then
diff --git a/modules b/modules
index 37d81c3..ba6f195 160000
--- a/modules
+++ b/modules
@@ -1 +1 @@
-Subproject commit 37d81c382ead3cd687505401d99e407c4b2a05c0
+Subproject commit ba6f195ecdd1b89cb52548fa8b0ada573a8c3a28-dirty
