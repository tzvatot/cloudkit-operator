/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	kubevirtv1 "kubevirt.io/api/core/v1"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/innabox/cloudkit-operator/api/v1alpha1"
	"github.com/innabox/cloudkit-operator/internal/helpers"
)

// getTenant gets the tenant object from the cluster
// If the tenant is not found, return nil and no error
func (r *ComputeInstanceReconciler) getTenant(ctx context.Context, instance *v1alpha1.ComputeInstance) (*v1alpha1.Tenant, error) {
	if instance.GetTenantReferenceName() == "" || instance.GetTenantReferenceNamespace() == "" {
		// tenant reference is not set because it doesn't exist yet
		return nil, nil
	}

	tenant := &v1alpha1.Tenant{}
	err := r.Get(ctx, client.ObjectKey{Namespace: instance.GetTenantReferenceNamespace(), Name: instance.GetTenantReferenceName()}, tenant)
	if err != nil {
		return nil, client.IgnoreNotFound(err)
	}

	return tenant, nil
}

// createOrUpdateTenant creates or updates the tenant object in the cluster in the namespace where the compute instance lives
func (r *ComputeInstanceReconciler) createOrUpdateTenant(ctx context.Context, instance *v1alpha1.ComputeInstance) error {
	tenantName, exists := instance.GetAnnotations()[cloudkitTenantAnnotation]
	if !exists || tenantName == "" {
		return fmt.Errorf("tenant name not found")
	}

	tenant := &v1alpha1.Tenant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getTenantObjectName(tenantName),
			Namespace: instance.GetNamespace(),
			Labels: map[string]string{
				"app.kubernetes.io/name": cloudkitAppName,
			},
		},
		Spec: v1alpha1.TenantSpec{
			Name: tenantName,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, tenant, func() error {
		err := controllerutil.SetOwnerReference(instance, tenant, r.Scheme)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Update the tenant reference
	instance.SetTenantReferenceName(tenant.Name)
	instance.SetTenantReferenceNamespace(tenant.GetNamespace())
	return nil
}

func getTenantObjectName(tenantName string) string {
	return fmt.Sprintf("tenant-%s", encodeTenantName(tenantName))
}

func labelSelectorFromComputeInstanceInstance(instance *v1alpha1.ComputeInstance) client.MatchingLabels {
	return client.MatchingLabels{
		cloudkitComputeInstanceNameLabel: instance.GetName(),
	}
}

// vmTemplateParameters represents the JSON structure of template parameters
type vmTemplateParameters struct {
	CPUCores        int    `json:"cpu_cores"`
	Memory          string `json:"memory"`
	DiskSize        string `json:"disk_size"`
	ImageSource     string `json:"image_source"`
	CloudInitConfig string `json:"cloud_init_config"`
	SSHPublicKey    string `json:"ssh_public_key,omitempty"`
	ExposedPorts    string `json:"exposed_ports"`
}

// parseTemplateParameters parses the template parameters from JSON
func parseTemplateParameters(templateParams string) (*vmTemplateParameters, error) {
	var params vmTemplateParameters
	if err := json.Unmarshal([]byte(templateParams), &params); err != nil {
		return nil, fmt.Errorf("failed to parse template parameters: %w", err)
	}

	// Validate required fields
	if params.CPUCores <= 0 {
		return nil, fmt.Errorf("cpu_cores must be greater than 0")
	}
	if params.Memory == "" {
		return nil, fmt.Errorf("memory is required")
	}
	if params.DiskSize == "" {
		return nil, fmt.Errorf("disk_size is required")
	}
	if params.ImageSource == "" {
		return nil, fmt.Errorf("image_source is required")
	}
	if params.CloudInitConfig == "" {
		return nil, fmt.Errorf("cloud_init_config is required")
	}
	if params.ExposedPorts == "" {
		return nil, fmt.Errorf("exposed_ports is required")
	}

	return &params, nil
}

// parseExposedPorts parses the exposed ports string into ServicePort objects
func parseExposedPorts(exposedPorts string) ([]corev1.ServicePort, error) {
	var ports []corev1.ServicePort
	portSpecs := strings.Split(exposedPorts, ",")

	for _, portSpec := range portSpecs {
		portSpec = strings.TrimSpace(portSpec)
		if portSpec == "" {
			continue
		}

		parts := strings.Split(portSpec, "/")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid port specification: %s", portSpec)
		}

		portNum, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid port number in %s: %w", portSpec, err)
		}

		protocol := corev1.Protocol(strings.ToUpper(parts[1]))
		if protocol != corev1.ProtocolTCP && protocol != corev1.ProtocolUDP {
			return nil, fmt.Errorf("invalid protocol in %s: must be tcp or udp", portSpec)
		}

		ports = append(ports, corev1.ServicePort{
			Name:     fmt.Sprintf("port-%d", portNum),
			Port:     int32(portNum),
			Protocol: protocol,
		})
	}

	return ports, nil
}

// createOrUpdateCloudInitSecret creates or updates the cloud-init secret
func (r *ComputeInstanceReconciler) createOrUpdateCloudInitSecret(
	ctx context.Context,
	instance *v1alpha1.ComputeInstance,
	namespace string,
	params *vmTemplateParameters,
) error {
	secretName := fmt.Sprintf("%s-cloud-init", instance.GetName())
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":         cloudkitAppName,
				cloudkitComputeInstanceNameLabel: instance.GetName(),
			},
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		secret.Type = corev1.SecretTypeOpaque
		secret.Data = map[string][]byte{
			"userdata": []byte(params.CloudInitConfig),
		}
		return nil
	})

	return err
}

// createOrUpdateSSHPublicKeySecret creates or updates the SSH public key secret (if provided)
func (r *ComputeInstanceReconciler) createOrUpdateSSHPublicKeySecret(
	ctx context.Context,
	instance *v1alpha1.ComputeInstance,
	namespace string,
	params *vmTemplateParameters,
) error {
	if params.SSHPublicKey == "" {
		return nil // SSH key is optional
	}

	secretName := fmt.Sprintf("%s-ssh-public-key", instance.GetName())
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":         cloudkitAppName,
				cloudkitComputeInstanceNameLabel: instance.GetName(),
			},
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		secret.Type = corev1.SecretTypeOpaque
		secret.Data = map[string][]byte{
			"key": []byte(params.SSHPublicKey),
		}
		return nil
	})

	return err
}

// createOrUpdateDataVolume creates or updates the DataVolume for the VM root disk
func (r *ComputeInstanceReconciler) createOrUpdateDataVolume(
	ctx context.Context,
	instance *v1alpha1.ComputeInstance,
	namespace string,
	params *vmTemplateParameters,
) error {
	dvName := fmt.Sprintf("%s-root-disk", instance.GetName())
	dv := &cdiv1beta1.DataVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dvName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":         cloudkitAppName,
				cloudkitComputeInstanceNameLabel: instance.GetName(),
			},
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dv, func() error {
		// Parse disk size
		diskSize, err := resource.ParseQuantity(params.DiskSize)
		if err != nil {
			return fmt.Errorf("failed to parse disk size: %w", err)
		}

		// Get storage class from environment or use default
		storageClassName := helpers.GetEnvWithDefault("CLOUDKIT_VM_OPERATIONS_STORAGE_CLASS", "")

		// Ensure image source has docker:// scheme prefix (required by CDI)
		imageURL := params.ImageSource
		if !strings.HasPrefix(imageURL, "docker://") {
			imageURL = "docker://" + imageURL
		}

		dv.Spec = cdiv1beta1.DataVolumeSpec{
			Source: &cdiv1beta1.DataVolumeSource{
				Registry: &cdiv1beta1.DataVolumeSourceRegistry{
					URL: ptr.To(imageURL),
				},
			},
			Storage: &cdiv1beta1.StorageSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: diskSize,
					},
				},
			},
		}

		if storageClassName != "" {
			dv.Spec.Storage.StorageClassName = &storageClassName
		}

		return nil
	})

	return err
}

// createOrUpdateKubeVirtVirtualMachine creates or updates the KubeVirt VirtualMachine
func (r *ComputeInstanceReconciler) createOrUpdateKubeVirtVirtualMachine(
	ctx context.Context,
	instance *v1alpha1.ComputeInstance,
	namespace string,
	params *vmTemplateParameters,
) error {
	vmName := instance.GetName()
	vm := &kubevirtv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vmName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":         cloudkitAppName,
				cloudkitComputeInstanceNameLabel: instance.GetName(),
			},
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, vm, func() error {
		// Parse memory
		memoryQuantity, err := resource.ParseQuantity(params.Memory)
		if err != nil {
			return fmt.Errorf("failed to parse memory: %w", err)
		}

		vm.Spec = kubevirtv1.VirtualMachineSpec{
			Running: ptr.To(true),
			Template: &kubevirtv1.VirtualMachineInstanceTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":         cloudkitAppName,
						cloudkitComputeInstanceNameLabel: instance.GetName(),
					},
				},
				Spec: kubevirtv1.VirtualMachineInstanceSpec{
					Domain: kubevirtv1.DomainSpec{
						CPU: &kubevirtv1.CPU{
							Cores: uint32(params.CPUCores),
						},
						Resources: kubevirtv1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: memoryQuantity,
							},
						},
						Devices: kubevirtv1.Devices{
							Disks: []kubevirtv1.Disk{
								{
									Name: "root-disk",
									DiskDevice: kubevirtv1.DiskDevice{
										Disk: &kubevirtv1.DiskTarget{
											Bus: kubevirtv1.DiskBusVirtio,
										},
									},
								},
								{
									Name: "cloud-init",
									DiskDevice: kubevirtv1.DiskDevice{
										Disk: &kubevirtv1.DiskTarget{
											Bus: kubevirtv1.DiskBusVirtio,
										},
									},
								},
							},
							Interfaces: []kubevirtv1.Interface{
								{
									Name: "default",
									InterfaceBindingMethod: kubevirtv1.InterfaceBindingMethod{
										Masquerade: &kubevirtv1.InterfaceMasquerade{},
									},
								},
							},
						},
						Features: &kubevirtv1.Features{
							SMM: &kubevirtv1.FeatureState{
								Enabled: ptr.To(true),
							},
							ACPI: kubevirtv1.FeatureState{},
							APIC: &kubevirtv1.FeatureAPIC{},
							Hyperv: &kubevirtv1.FeatureHyperv{
								Relaxed: &kubevirtv1.FeatureState{
									Enabled: ptr.To(true),
								},
								VAPIC: &kubevirtv1.FeatureState{
									Enabled: ptr.To(true),
								},
								Spinlocks: &kubevirtv1.FeatureSpinlocks{
									Enabled: ptr.To(true),
									Retries: ptr.To(uint32(8191)),
								},
							},
						},
					},
					Networks: []kubevirtv1.Network{
						{
							Name: "default",
							NetworkSource: kubevirtv1.NetworkSource{
								Pod: &kubevirtv1.PodNetwork{},
							},
						},
					},
					Volumes: []kubevirtv1.Volume{
						{
							Name: "root-disk",
							VolumeSource: kubevirtv1.VolumeSource{
								DataVolume: &kubevirtv1.DataVolumeSource{
									Name: fmt.Sprintf("%s-root-disk", vmName),
								},
							},
						},
						{
							Name: "cloud-init",
							VolumeSource: kubevirtv1.VolumeSource{
								CloudInitNoCloud: &kubevirtv1.CloudInitNoCloudSource{
									UserDataSecretRef: &corev1.LocalObjectReference{
										Name: fmt.Sprintf("%s-cloud-init", vmName),
									},
								},
							},
						},
					},
				},
			},
		}

		// Add SSH public key access credentials if provided
		if params.SSHPublicKey != "" {
			vm.Spec.Template.Spec.AccessCredentials = []kubevirtv1.AccessCredential{
				{
					SSHPublicKey: &kubevirtv1.SSHPublicKeyAccessCredential{
						Source: kubevirtv1.SSHPublicKeyAccessCredentialSource{
							Secret: &kubevirtv1.AccessCredentialSecretSource{
								SecretName: fmt.Sprintf("%s-ssh-public-key", vmName),
							},
						},
						PropagationMethod: kubevirtv1.SSHPublicKeyAccessCredentialPropagationMethod{
							QemuGuestAgent: &kubevirtv1.QemuGuestAgentSSHPublicKeyAccessCredentialPropagation{
								Users: []string{"fedora", "cloud-user", "ubuntu"},
							},
						},
					},
				},
			}
		}

		return nil
	})

	return err
}

// createOrUpdateLoadBalancerService creates or updates the LoadBalancer service
func (r *ComputeInstanceReconciler) createOrUpdateLoadBalancerService(
	ctx context.Context,
	instance *v1alpha1.ComputeInstance,
	namespace string,
	params *vmTemplateParameters,
) error {
	serviceName := fmt.Sprintf("%s-load-balancer", instance.GetName())
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":         cloudkitAppName,
				cloudkitComputeInstanceNameLabel: instance.GetName(),
			},
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		// Parse exposed ports
		ports, err := parseExposedPorts(params.ExposedPorts)
		if err != nil {
			return err
		}

		service.Spec = corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Selector: map[string]string{
				cloudkitComputeInstanceNameLabel: instance.GetName(),
			},
			Ports: ports,
		}

		return nil
	})

	return err
}

// provisionComputeInstanceResources orchestrates the creation of all VM resources
func (r *ComputeInstanceReconciler) provisionComputeInstanceResources(
	ctx context.Context,
	instance *v1alpha1.ComputeInstance,
	tenant *v1alpha1.Tenant,
) error {
	// Parse template parameters
	params, err := parseTemplateParameters(instance.Spec.TemplateParameters)
	if err != nil {
		return fmt.Errorf("failed to parse template parameters: %w", err)
	}

	// Determine target namespace (tenant's actual namespace from status)
	namespace := tenant.Status.Namespace

	// Create cloud-init secret
	if err := r.createOrUpdateCloudInitSecret(ctx, instance, namespace, params); err != nil {
		return fmt.Errorf("failed to create cloud-init secret: %w", err)
	}

	// Create SSH public key secret (if provided)
	if err := r.createOrUpdateSSHPublicKeySecret(ctx, instance, namespace, params); err != nil {
		return fmt.Errorf("failed to create SSH public key secret: %w", err)
	}

	// Create DataVolume
	if err := r.createOrUpdateDataVolume(ctx, instance, namespace, params); err != nil {
		return fmt.Errorf("failed to create DataVolume: %w", err)
	}

	// Create KubeVirt VirtualMachine
	if err := r.createOrUpdateKubeVirtVirtualMachine(ctx, instance, namespace, params); err != nil {
		return fmt.Errorf("failed to create VirtualMachine: %w", err)
	}

	// Create LoadBalancer service
	if err := r.createOrUpdateLoadBalancerService(ctx, instance, namespace, params); err != nil {
		return fmt.Errorf("failed to create LoadBalancer service: %w", err)
	}

	return nil
}
