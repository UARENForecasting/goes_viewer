from setuptools import setup
import versioneer


setup(
    name='goes_viewer',
    description='Model for converting GOES files to RGB for bokeh displays',
    author='Antonio Lorenzo',
    author_email='atlorenzo@email.arizona.edu',
    license='MIT',
    packages=['goes_viewer'],
    zip_safe=True,
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass()
)
