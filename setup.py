from setuptools import setup, find_packages
import versioneer


setup(
    name='goes_viewer',
    description='Model for converting GOES files to RGB for bokeh displays',
    author='Antonio Lorenzo',
    author_email='atlorenzo@email.arizona.edu',
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=True,
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    entry_points={
        'console_scripts': [
            'goes-viewer=goes_viewer.cli:cli'
        ]
    },

)
