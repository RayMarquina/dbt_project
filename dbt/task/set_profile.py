import pprint
import yaml

class SetProfileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def update_local_config(self, data):
        local_config = self.project.load_local_config()
        local_config.update(data)
        return local_config

    def write_local_config(self, config):
        with open('.dbt.yml', 'w') as local_config_fh:
            config_contents = yaml.dump(config)
            local_config_fh.write(config_contents)

    def validate_profiles(self, profiles):
        existing_profiles = self.project.profiles.keys()

        invalid_profiles = []
        for profile in profiles:
            if profile not in existing_profiles:
                invalid_profiles.append(profile)
        return invalid_profiles

    def run(self):
        profiles = self.args.profiles
        invalid_profiles = self.validate_profiles(profiles)

        if len(invalid_profiles) == 0:
            config = self.update_local_config({'profiles': profiles})
            self.write_local_config(config)

            profiles_str = ", ".join('"{}"'.format(profile) for profile in profiles)
            print("Set environemnt to use profiles: [{}]".format(profiles_str))
        else:

            invalid_profiles_str = ", ".join('"{}"'.format(profile) for profile in invalid_profiles)
            print("ERROR: One or more specified profiles do not exist! [{}]".format(invalid_profiles_str))
