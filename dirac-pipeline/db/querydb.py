
# that will eventually be replaced by subprocess.Popen() call
output = open("output.log","r").read()
sites_raw = [s.split("\n") for s in output.split("Name of the CE: ")]
sites = {item[0]:[key.replace("\t","") for key in item[1:-1] if len(key)!=0] for item in sites_raw if len(item[0])!=0}

# that's the proto-output of lcg-infosites --vo glast.org tag
#for site in sorted(sites):
#    print 'Name of site %s:'%site
#    for release in sites[site]:
#        print '\t%s'%release
#    print '\n'

# now i need a list of Dirac sites that match the CE
