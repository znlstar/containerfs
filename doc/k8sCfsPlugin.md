# k8s PV&PVC支持cfs插件

## 创建卷

1. 用户创建cfs volume


2. 用户创建PersistentVolume

```
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pv-cfs
  labels:
    app: shcapp
spec:
  capacity:
    storage: 10Gi
  accessModes:
    - ReadWriteMany
  containerfs:
    volmgr: "192.168.100.216:10001"
    metanode: "192.168.100.17:10002,192.168.100.17:10003,192.168.100.17:10004"
    uuid: 72f32fe240548ae880e27446f49f9935
    readOnly: false
```

一个PersistentVolume对应cfs的一个volume，需要指定containerfs及参数volmgr, metanode, uuid, readOnly（可选，默认为false）。



3. 用户创建PersistentVolumeClaim请求

```
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: claim-cfs
  namespace: shc
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 10G
```

以上创建了一个容量为10G的请求，系统会为之绑定合适的PersistentVolume，即上一步中的“pv-cfs”。



## 挂载卷 

```
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: deploy-0
  namespace: shc
  labels:
    name: deploy
spec:
  replicas: 3
  template:
    metadata:
      labels:
        name: deploy
    spec:
      containers:
        - name: test
          image: busybox
          volumeMounts:
          - mountPath: "/export/Logs"
            name: logs
      volumes:
        - name: logs
          persistentVolumeClaim:
           claimName: claim-cfs
```

在创建pod/RC/RS/Deployment时，指定需要挂载的PersistentVolumeClaim。



## 动态持久化卷

上述方式需要用户依次创建cfs volume，PersistentVolume，PersistentVolumeClaim。除了这种方式，还可以通过动态创建的方式，跳过手动创建PersistentVolume的过程，根据PersistentVolumeClaim和StorageClass创建PersistentVolume和cfs volume。

1. 管理员配置StorageClass对象

```
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: standard
provisioner: kubernetes.io/containerfs
parameters:
  metanode: "192.168.100.17:9903,192.168.100.18:9913,192.168.100.19:9923"
  volmgr: "192.168.100.216:10001"
```

以上创建了一个名为standard的StorageClass对象，该对象指定存储插件为kubernetes.io/containerfs，并设置了containerfs需要的参数metanode, volmgr。



2. 用户创建PersistentVolumeClaim请求

```
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: claim-cfs1
  namespace: shc
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 10G
  storageClassName: standard
```

以上创建了一个容量为10G的请求，并指定standard StorageClass来动态创建。



3. 查看已经创建好的卷

```
[root@node-216 shc]# kubectl -nshc1 get pvc
NAME           STATUS    VOLUME              CAPACITY   ACCESSMODES   STORAGECLASS   AGE
claim-cfs1     Bound     pvc-ecf22d59-af13   10Gi       RWX           standard       16s
```

系统为用户请求的“claim-cfs1”创建了一个PersistentVolume “pvc-ecf22d59-af13”，并绑定成功。





