from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

setup(name='pysmosis',
      version='0.0.1',
      description='Python Open Street Map tools',
      packages=['pysmosis', 'pysmosis.sqlite'],
      cmdclass = {'build_ext': build_ext},
      ext_modules = [Extension("pysmosis.geom.linearref", ["pysmosis/geom/linearref.pyx"])]
     )
