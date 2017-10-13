package containerfs

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"strconv"
	"time"
	dstrings "strings"
	"math/rand"
	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubernetes/pkg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	storagev1 "k8s.io/kubernetes/pkg/apis/storage/v1"
	"k8s.io/kubernetes/pkg/util/exec"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/util/strings"
	"k8s.io/kubernetes/pkg/volume"
	volutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/client/clientset_generated/clientset"
)

func ProbeVolumePlugins() []volume.VolumePlugin{
	return []volume.VolumePlugin{&containerfsPlugin{host: nil, exe: exec.New()}}
}

type containerfsPlugin struct {
	host volume.VolumeHost
	exe exec.Interface
}

var _ volume.VolumePlugin = &containerfsPlugin{}
var _ volume.PersistentVolumePlugin = &containerfsPlugin{}
var _ volume.DeletableVolumePlugin = &containerfsPlugin{}
var _ volume.ProvisionableVolumePlugin = &containerfsPlugin{}


const (
	containerfsPluginName         = "kubernetes.io/containerfs"

	containerfsBinariesPath       = "/usr/libexec/kubernetes/kubelet-plugins/volume/containerfs/cfs-CLI"
	containerfsCreateCommand      = "createvol"
	containerfsDeleteCommand      = "deletevol"
	containerfsMountBinariesPath  = "/usr/libexec/kubernetes/kubelet-plugins/volume/containerfs/cfs-fuseclient"

	volumeAllocationUnit          = 10
	volumeUUidLength              = 32
)

func (plugin *containerfsPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	return nil
}

func (plugin *containerfsPlugin) GetPluginName() string {
	return containerfsPluginName
}

func (plugin *containerfsPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"%v:%v",
		volumeSource.Uuid,
		volumeSource.Uuid), nil
}

func (plugin *containerfsPlugin) CanSupport(spec *volume.Spec) bool {
	if (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Containerfs == nil) ||
		(spec.Volume != nil && spec.Volume.Containerfs == nil) {
		return false
	}
	return true
}

func (plugin *containerfsPlugin) RequiresRemount() bool {
	return false
}

func (plugin *containerfsPlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {
	return []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
		v1.ReadOnlyMany,
		v1.ReadWriteMany,
	}
}

func (plugin *containerfsPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod, _ volume.VolumeOptions) (volume.Mounter, error) {
	return plugin.newMounterInternal(spec, pod, plugin.host.GetMounter())
}

func (plugin *containerfsPlugin) getContainerVolumeSource(spec *volume.Spec) (*v1.ContainerfsVolumeSource, bool) {
	// Containerfs volumes used directly in a pod have a ReadOnly flag set by the pod author.
	// Containerfs volumes used as a PersistentVolume gets the ReadOnly flag indirectly through the persistent-claim volume used to mount the PV
	if spec.Volume != nil && spec.Volume.Containerfs != nil {
		return spec.Volume.Containerfs, spec.Volume.Containerfs.ReadOnly
	} else {
		return spec.PersistentVolume.Spec.Containerfs, spec.ReadOnly
	}
}

func (plugin *containerfsPlugin) newMounterInternal(spec *volume.Spec, pod *v1.Pod, mounter mount.Interface) (volume.Mounter, error) {
	source, readOnly := plugin.getContainerVolumeSource(spec)
	return &containerfsMounter{
		containerfs: &containerfs{
			volName: spec.Name(),
			mounter: mounter,
			pod:     pod,
			plugin:  plugin,
		},
		volmgr:   source.Volmgr,
		metanode: source.Metanode,
		uuid:     source.Uuid,
		readOnly: readOnly,
		mountOptions: volume.MountOptionFromSpec(spec),
	}, nil
}

func (plugin *containerfsPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return plugin.newUnmounterInternal(volName, podUID, plugin.host.GetMounter())
}

func (plugin *containerfsPlugin) newUnmounterInternal(volName string, podUID types.UID, mounter mount.Interface) (volume.Unmounter, error) {
	return &containerfsUnmounter{&containerfs{
		volName: volName,
		mounter: mounter,
		pod:     &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID}},
		plugin:  plugin,
	}}, nil
}

func (plugin *containerfsPlugin) ConstructVolumeSpec(volumeName, mountPath string) (*volume.Spec, error) {
	containerfsVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			Containerfs: &v1.ContainerfsVolumeSource{
				Uuid:          volumeName,
			},
		},
	}
	return volume.NewSpecFromVolume(containerfsVolume), nil
}

func (plugin *containerfsPlugin) SupportsMountOption() bool {
	return true
}

func (plugin *containerfsPlugin) SupportsBulkVolumeVerification() bool {
	return false
}

func getVolumeSource(spec *volume.Spec) (*v1.ContainerfsVolumeSource, bool, error) {
	if spec.Volume != nil && spec.Volume.Containerfs != nil {
		return spec.Volume.Containerfs, spec.Volume.Containerfs.ReadOnly, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.Containerfs != nil {
		return spec.PersistentVolume.Spec.Containerfs, spec.ReadOnly, nil
	}
	return nil, false, fmt.Errorf("Spec does not reference a ContainerFS volume type")
}

func (plugin *containerfsPlugin) execCommand(command string, args []string) ([]byte, error) {
	cmd := plugin.exe.Command(command, args...)
	glog.V(4).Infof("containerfs: exec command %s %v", command, args)
	return cmd.CombinedOutput()
}

// Containerfs volumes represent a bare host file or directory mount of an Containerfs export.
type containerfs struct {
	volName string
	pod     *v1.Pod
	mounter mount.Interface
	plugin  *containerfsPlugin
	volume.MetricsNil
}

type containerfsMounter struct {
	*containerfs
	volmgr       string
	metanode     string
	uuid         string
	readOnly     bool
	mountOptions []string
}

var _ volume.Mounter = &containerfsMounter{}


func (b *containerfsMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:        b.readOnly,
		Managed:         false,
		SupportsSELinux: false,
	}
}

// Checks prior to mount operations to verify that the required components (binaries, etc.)
// to mount the volume are available on the underlying node.
// If not, it returns an error
func (b *containerfsMounter) CanMount() error {
	switch runtime.GOOS {
	case "linux":
		out, err := b.plugin.execCommand("/bin/ls", []string{containerfsMountBinariesPath});
		if err != nil {
			return fmt.Errorf("Required binary is missing. %s. %s", string(out), err)
		}

	}
	return nil
}

// SetUp attaches the disk and bind mounts to the volume path.
func (b *containerfsMounter) SetUp(fsGroup *int64) error {
	return b.SetUpAt(b.GetPath(), fsGroup)
}

func (b *containerfsMounter) SetUpAt(dir string, fsGroup *int64) error {
	notMnt, err := b.mounter.IsLikelyNotMountPoint(dir)
	glog.V(4).Infof("containerfs: mount set up: %s %v %v", dir, !notMnt, err)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if !notMnt {
		return nil
	}

	os.MkdirAll(dir, 0750)
	err = b.setUpAtInternal(dir)
	if err == nil {
		return nil
	}

	// Cleanup upon failure.
	volutil.UnmountPath(dir, b.mounter)
	return err
}

func (b *containerfsMounter) setUpAtInternal(dir string) error {
	var errs error
	options := []string{}
	options = append(options, "-uuid=" + b.uuid)
	options = append(options, "-mountpoint=" + dir)
	options = append(options, "-volmgr=" + b.volmgr)
	options = append(options, "-metanode=" + b.metanode)
	// TODO readOnly option is not support now.
	//if b.readOnly {
	//	options = append(options, "ro")
	//}

	p := path.Join(b.containerfs.plugin.host.GetPluginDir(containerfsPluginName), b.containerfs.volName)
	if err := os.MkdirAll(p, 0750); err != nil {
		return fmt.Errorf("containerfs: mkdir failed: %v", err)
	}

	// adding log-level ERROR to remove noise
	// and more specific log path so each pod has
	// its own log based on PV + Pod
	log := path.Join(p, b.pod.Name+"-log")
	options = append(options, "-loglevel=error")
	options = append(options, "-log="+log)

	mountOptions := volume.JoinMountOptions(b.mountOptions, options)

	glog.V(4).Infof("containerfs: mountOptions %v", mountOptions)
	errCh := make(chan error, 1)
	go func() {
		_, errs = b.plugin.execCommand(containerfsMountBinariesPath, mountOptions)
		if errs != nil {
			errCh <- errs
		}
	}()

	time.Sleep(500*time.Millisecond)
	select {
	case err := <-errCh:
		glog.V(4).Infof("containerfs: err return: %v", err)
		if err != nil {
			return fmt.Errorf("containerfs: mount failed: %v", err)
		}
	default:
		glog.V(4).Infof("containerfs: no err return")
	}

	glog.V(4).Infof("containerfs: successfully mounted %s", dir)
	return nil
}

func (containerfsVolume *containerfs) GetPath() string {
	name := containerfsPluginName
	return containerfsVolume.plugin.host.GetPodVolumeDir(containerfsVolume.pod.UID, strings.EscapeQualifiedNameForDisk(name), containerfsVolume.volName)
}

type containerfsUnmounter struct {
	*containerfs
}

var _ volume.Unmounter = &containerfsUnmounter{}

func (c *containerfsUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

func (c *containerfsUnmounter) TearDownAt(dir string) error {
	return volutil.UnmountPath(dir, c.mounter)
}

func (plugin *containerfsPlugin) NewProvisioner(options volume.VolumeOptions) (volume.Provisioner, error) {
	return plugin.newProvisionerInternal(options)
}

func (plugin *containerfsPlugin) newProvisionerInternal(options volume.VolumeOptions) (volume.Provisioner, error) {
	return &containerfsVolumeProvisioner{
		containerfs: &containerfs{
			plugin: plugin,
		},
		options: options,
	}, nil
}

type provisionerConfig struct {
	volmgr             string
	metanode           string
}

type containerfsVolumeProvisioner struct {
	*containerfs
	provisionerConfig
	options volume.VolumeOptions
}

var _ volume.Provisioner = &containerfsVolumeProvisioner{}

func (r *containerfsVolumeProvisioner) Provision() (*v1.PersistentVolume, error) {
	if r.options.PVC.Spec.Selector != nil {
		glog.V(4).Infof("containerfs: not able to parse your claim Selector")
		return nil, fmt.Errorf("containerfs: not able to parse your claim Selector")
	}
	glog.V(4).Infof("containerfs: Provison VolumeOptions %v", r.options)
	cfg, err := parseClassParameters(r.plugin.host.GetKubeClient(), r.options.Parameters)
	if err != nil {
		return nil, err
	}
	r.provisionerConfig = *cfg
	glog.V(4).Infof("containerfs: creating volume with configuration %+v", r.provisionerConfig)

	containerfs, sizeGB, err := r.CreateVolume()
	if err != nil {
		glog.Errorf("containerfs: create volume err: %v.", err)
		return nil, fmt.Errorf("containerfs: create volume err: %v.", err)
	}
	pv := new(v1.PersistentVolume)
	pv.Spec.PersistentVolumeSource.Containerfs = containerfs
	pv.Spec.PersistentVolumeReclaimPolicy = r.options.PersistentVolumeReclaimPolicy
	pv.Spec.AccessModes = r.options.PVC.Spec.AccessModes
	if len(pv.Spec.AccessModes) == 0 {
		pv.Spec.AccessModes = r.plugin.GetAccessModes()
	}
	pv.Spec.Capacity = v1.ResourceList{
		v1.ResourceName(v1.ResourceStorage): resource.MustParse(fmt.Sprintf("%dGi", sizeGB)),
	}
	return pv, nil
}

func (plugin *containerfsPlugin) NewDeleter(spec *volume.Spec) (volume.Deleter, error) {
	return plugin.newDeleterInternal(spec)
}

func (plugin *containerfsPlugin) newDeleterInternal(spec *volume.Spec) (volume.Deleter, error) {
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Containerfs == nil {
		return nil, fmt.Errorf("spec.PersistentVolumeSource.Spec.Containerfs is nil")
	}
	return &containerfsVolumeDeleter{
		containerfs: &containerfs{
			volName: spec.Name(),
			plugin:  plugin,
		},
		spec: spec.PersistentVolume,
	}, nil
}

type containerfsVolumeDeleter struct {
	*containerfs
	spec *v1.PersistentVolume
}

var _ volume.Deleter = &containerfsVolumeDeleter{}

func (d *containerfsVolumeDeleter) GetPath() string {
	name := containerfsPluginName
	return d.plugin.host.GetPodVolumeDir(d.pod.UID, strings.EscapeQualifiedNameForDisk(name), d.volName)
}

func (d *containerfsVolumeDeleter) Delete() error {
	volumeId := d.spec.Spec.Containerfs.Uuid
	glog.V(4).Infof("containerfs: deleting volume %s", volumeId)

	options := []string{}
	options = append(options, "-volmgr=" + d.spec.Spec.Containerfs.Volmgr)
	options = append(options, "-metanode=" + d.spec.Spec.Containerfs.Metanode)
	options = append(options, containerfsDeleteCommand)
	options = append(options, volumeId)
	output, err := d.plugin.execCommand(containerfsBinariesPath, options)
	if err != nil || dstrings.Contains(string(output), "failed") {
		glog.Errorf("containerfs: error deleting volume %v:%s ", err, string(output))
		return fmt.Errorf("containerfs: error deleting volume %v:%s ", err, string(output))
	}

	glog.V(2).Infof("containerfs: volume %s deleted successfully", volumeId)
	return nil
}

func (p *containerfsVolumeProvisioner) CreateVolume() (r *v1.ContainerfsVolumeSource, size int, err error) {
	capacity := p.options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	volSizeBytes := capacity.Value()
	sz := int(volume.RoundUpSize(volSizeBytes, volumeAllocationUnit*1024*1024*1024))
	sizeGB := sz * volumeAllocationUnit
	glog.V(2).Infof("containerfs: create volume of size: %d Gi", sizeGB)

	options := []string{}
	options = append(options, "-volmgr=" + p.volmgr)
	options = append(options, "-metanode=" + p.metanode)
	options = append(options, containerfsCreateCommand)
	options = append(options, p.options.PVName)
	options = append(options, strconv.Itoa(sizeGB))
	output, err := p.plugin.execCommand(containerfsBinariesPath, options)
	uuid := dstrings.Trim(string(output), "\n")
	if err != nil || dstrings.Contains(uuid, "failed") || len(uuid) != volumeUUidLength {
		glog.Errorf("containerfs: error creating volume %v:%s", err, uuid)
		return nil, 0, fmt.Errorf("error creating volume %v:%s", err, uuid)
	}
	glog.V(4).Infof("containerfs: creating volume successfully. %s", uuid)

	return &v1.ContainerfsVolumeSource{
		Volmgr:        p.volmgr,
		Metanode:      p.metanode,
		Uuid:          uuid,
		ReadOnly:      false,
	}, sizeGB, nil
}

func parseClassParameters(kubeClient clientset.Interface, params map[string]string) (*provisionerConfig, error) {
	var cfg provisionerConfig

	if len(params) == 0 {
		storageClassList, err := kubeClient.Storage().StorageClasses().List(metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("containerfs: get storageclasses err: %v", err)
		}
		selectedClass, err := selectStorageClass(storageClassList.Items)
		if err != nil {
			return nil, err
		}
		glog.V(4).Infof("containerfs: selected storageclass %s", selectedClass.Name)
		params = selectedClass.Parameters
	}

	for k, v := range params {
		switch dstrings.ToLower(k) {
		case "volmgr":
			cfg.volmgr = v
		case "metanode":
			cfg.metanode = v
		default:
			return nil, fmt.Errorf("containerfs: invalid option %q for volume plugin %s", k, containerfsPluginName)
		}
	}

	if len(cfg.volmgr) == 0 {
		return nil, fmt.Errorf("StorageClass for provisioner %s must contain 'volmgr' parameter", containerfsPluginName)
	}
	if len(cfg.metanode) == 0 {
		return nil, fmt.Errorf("StorageClass for provisioner %s must contain 'metanode' parameter", containerfsPluginName)
	}
	return &cfg, nil
}

func selectStorageClass(classes []storagev1.StorageClass) (*storagev1.StorageClass, error) {
	filteredClasses := []storagev1.StorageClass{}
	for _, class := range classes {
		if class.Provisioner != containerfsPluginName {
			continue
		}
		if len(class.Parameters) == 0 {
			continue
		}
		filteredClasses = append(filteredClasses, class)
	}
	if len(filteredClasses) == 0 {
		return nil, fmt.Errorf("containerfs: cannot find storageclass for volume plugin %+v", containerfsPluginName)
	}
	index := rand.Int() % len(filteredClasses)
	return &filteredClasses[index], nil
}
