from typing import Any, Optional
import os
import pickle


class ObjPersist:
    def __init__(self,
                 store_dir: str,
                 group: str):
        self.store_dir = store_dir
        self.group = group

    def persist(self,
                data: Any,
                name: str,
                tag: str
                ):
        file_path = os.path.join(self.store_dir, self.group, name)
        if not os.path.exists(file_path):
            os.makedirs(file_path)

        file_name = os.path.join(file_path, f"{tag}.p")
        with open(file_name, "wb") as f:
            pickle.dump(data, f)

    def restore(self,
                name: str,
                tag: str,
                group: Optional[str] = None) -> Any:
        error = None
        data = None
        file_path = os.path.join(self.store_dir, self.group, name, f"{tag}.p")
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
        else:
            error = f"{name} {tag} does not exist"

        return error, data


if __name__ == '__main__':
    from datetime import datetime

    my_data = {'key_1': 123,
               'key_2': {'key_3': 'hello'}}

    obj_persist = ObjPersist(store_dir='../data',
                             group='test_group')

    tag = f"t_{datetime.utcnow().timestamp()}"
    obj_persist.persist(data=my_data,
                        name='test_data',
                        tag=tag)

    restored_data = obj_persist.restore(name='test_data',
                                        tag=tag)

    pass