from distutils.core import setup


setup(
    name='ips_services',

    version='0.0.1',

    description='Legacy Uplift version of the IPS system',

    author='Social Surveys',

    author_email='social.surveys@ons.gsi.gov.uk',

    license='MIT',

    packages=['ips_services'],

    package_data={
        '': ['*.yaml', '*.r'],
    }
)
