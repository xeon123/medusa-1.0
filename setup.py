from distutils.core import setup

#This is a list of files to install, and where
#(relative to the 'root' dir, where setup.py is)
#You could be more specific.
files = ["things/*"]

setup(name = "medusa_hadoop",
      version = "1",
      description = "Hadoop MapReduce (MR) Byzantine-fault-tolerant (BFT) manager for Cloud-of-clouds (CoC)",
      author = "xeon",
      author_email = "xeonmailinglist@gmail.com",
      url = "https://bitbucket.org/pcosta_pt/medusa_hadoop",
      #Name the folder where your packages live:
      #(If you have other packages (dirs) or modules (py files) then  put them into the package directory - they will be found recursively.)
      packages = ['manager', 'tests'],
      #package_data={'': ['bin/*.sh']},
      #'package' package must contain files (see list above)
      #I called the package 'package' thus cleverly confusing the whole issue...
      #This dict maps the package name =to=> directories
      #It says, package *needs* these files.
      #package_data = {'package' : files },
      #'runner' is in the root.
      #scripts = ["bin"],
      long_description = """This is the hadoop MR manager programs that manage hadoop MR job execution in a cloud-of-clouds environment. This app also can tolerate arbitrary faults, manage clouds and job execution and also offern algorithms from job scheduling."""
      #This next part it for the Cheese Shop, look a little down the page.
      #classifiers = []
)
