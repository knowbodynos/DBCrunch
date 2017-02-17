#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import subprocess,signal;

def default_sigpipe():
    signal.signal(signal.SIGPIPE,signal.SIG_DFL);

def pendlicensecount(username,modlist,modulesdirpath,softwarestatefile):
	grepmods="|".join(modlist);
	#pendjobnames=subprocess.Popen("squeue -h -u "+username+" -o '%T %j' | grep 'PENDING' | cut -d' ' -f2 | | grep -E \"("+grepmods+")\" | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(",");
	pendjobnames=["involution_4_job_10_steps_289-320","involution_4_job_11_steps_321-352","involution_4_job_12_steps_353-384","involution_4_job_13_steps_385-416","involution_4_job_14_steps_417-448","involution_4_job_15_steps_449-480","involution_4_job_16_steps_481-512","involution_4_job_17_steps_513-544","involution_4_job_18_steps_545-576","involution_4_job_19_steps_577-608","involution_4_job_1_steps_1-32","involution_4_job_20_steps_609-640","involution_4_job_21_steps_641-672","involution_4_job_22_steps_673-704","involution_4_job_23_steps_705-736","involution_4_job_24_steps_737-768","involution_4_job_2_steps_33-64","involution_4_job_3_steps_65-96","involution_4_job_4_steps_97-128","involution_4_job_5_steps_129-160","involution_4_job_6_steps_161-192","involution_4_job_7_steps_193-224","involution_4_job_8_steps_225-256","involution_4_job_9_steps_257-288"];
	npendjobsteps=0;
	npendjobthreads=0;
	for pjn in pendjobnames:
		pjnsplit=pjn.split("_");
		modname=pjnsplit[0];
		controllername=pjnsplit[1];
		nsteps=1-eval(pjnsplit[5]);
		scriptlanguage=subprocess.Popen("cat "+modulesdirpath+"/"+modname+"/"+controllername+"/controller_"+modname+"_"+controllername+".job | grep 'scriptlanguage=' | cut -d'=' -f2 | cut -d'\"' -f2 | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];
		needslicense=eval(subprocess.Popen("cat "+softwarestatefile+" | grep \""+scriptlanguage+"\" | cut -d',' -f2 | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0]);
		njobthreads=eval(subprocess.Popen("echo \"$(cat "+modulesdirpath+"/"+modname+"/"+controllername+"/jobs/"+pjn+".job | grep -E \"njobstepthreads\[[0-9]+\]=\" | cut -d'=' -f2 | tr '\n' '+' | head -c -1)\" | bc | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0]);
		npendjobsteps+=nsteps;
		npendjobthreads+=njobthreads;
	return [npendjobsteps,npendjobthreads];

username="altman.ro";
mainpath="/gss_gpfs_scratch/altman.ro";
packagepath=mainpath+"/ToricCY"
statepath=packagepath+"/state";
softwarestatefile=statepath+"/software";
modulesdirpath=mainpath+"/modules";
modlist=["triangulate","triangulatecones","gluegeometries","toricswisscheese","involution","orientifoldMat","orientifold"];

print pendlicensecount(username,modlist,modulesdirpath,softwarestatefile);